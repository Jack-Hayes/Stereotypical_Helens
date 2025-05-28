"""
<Module: LAZ to DSM Pipeline>

This module provides functions to:

1. **On-demand CRS Determination**
   - `detect_input_crs_wkt(laz_path)`: uses PDAL metadata to read the source LAS/LAZ file's spatial reference WKT.
   - `detect_output_utm_crs_wkt(laz_path)`: computes the file's centroid from PDAL bbox, transforms to lat/lon, 
                                            auto-selects the appropriate UTM zone (WGS84), and builds a 3D compound CRS with ellipsoidal height.

2. **PDAL Pipeline Construction**
   - `create_pdal_pipeline(...)`:
     - Clips the point cloud to a user-supplied AOI (GeoDataFrame/GeoSeries in EPSG:4326).
     - Applies classification filters in order: reset → SMRF (optional) or returns → low/high noise → road → ground-only.
     - Optionally reprojects only when source and target CRSs differ.
     - Optionally writes the filtered point cloud to LAS or LAZ.
     - **Always** appends the `filter_percentile.py` outlier filter last (uses IDW interpolated Z thresholds).

3. **DSM Writing**
   - `create_dsm_stage(...)`: defines a `writers.gdal` stage to generate a digital surface model GeoTIFF (IDW), with LZW compression, 
      tiling, overview copying, and a `Z>0` where clause.

All WKT output uses WKT2 compound syntax for compatibility with PDAL reprojection.

Example usage in Jupyter:
```python
input_wkt = detect_input_crs_wkt("/path/to/file.laz")
output_wkt = detect_output_utm_crs_wkt("/path/to/file.laz")
pipeline = create_pdal_pipeline(
    laz_file, aoi_gdf,
    input_crs=input_wkt,
    output_crs=output_wkt,
    filter_low_noise=True,
    group_filter="first,only",
    save_pointcloud=False
)
dsm_stage = create_dsm_stage("out_dsm.tif", gridmethod="idw", dimension="Z")
pipeline += dsm_stage
p = pdal.Pipeline(json.dumps({"pipeline": pipeline}))
p.execute()
```
"""
import json
import os
import pdal
from pyproj import CRS, Transformer


# -----------------------------------------------------------------------------
# 1) CRS Detection Functions
# -----------------------------------------------------------------------------
def detect_input_crs_wkt(laz_path):
    """
    Reads the spatial reference WKT from a .laz LAS/LAZ dataset using PDAL metadata.
    Returns a WKT string of the input CRS.
    """
    # Minimal pipeline to read header metadata
    pipeline_json = {"pipeline": [{"type": "readers.las", "filename": laz_path}]}
    pipeline = pdal.Pipeline(json.dumps(pipeline_json))
    pipeline.execute()

    # Extract PDAL reader metadata
    md = pipeline.metadata['metadata']['readers.las']

    # Prefer direct 'spatialreference' WKT
    if 'spatialreference' in md and md['spatialreference']:
        return md['spatialreference']
    # Otherwise fallback to compound WKT under 'srs'
    if 'srs' in md and 'compoundwkt' in md['srs']:
        return md['srs']['compoundwkt']

    raise ValueError(f"No valid SRS WKT found in PDAL metadata for: {laz_path}")


def find_longitude_of_origin_from_utm(epsg_code):
    """
    Given a UTM EPSG code (e.g. 32610 or 32710), compute the central meridian longitude.
    Formula: (zone_number - 1)*6 - 180 + 3
    """
    zone = epsg_code % 100  # last two digits
    return (zone - 1) * 6 - 180 + 3


def detect_output_utm_crs_g2139_wkt(laz_path, base_utm_wkt_path):
    """
    Auto-detects UTM zone from LAZ centroid and builds a WGS84 G2139-based 3D compound CRS.
    Reads a base WKT file (for a specific zone) then replaces zone string and central meridian,
    writes new WKT to /tmp/UTM_<zone>_WGS84_G2139_3D.wkt, and returns that path.

    Parameters:
    - laz_path: input LAZ file for centroid-based zone detection
    - base_utm_wkt_path: path to base WKT template (e.g. UTM_10N_WGS84_G2139_3D.wkt)

    Credit to Shashank Bhushan's 
    https://github.com/uw-cryo/wv_stereo_processing/blob/fbd1fcadf7e81b75b506e2e66d3f817eea568d09/scripts/csm_proc/produce_colocated_dem_ortho_large_clouds.py#L294
    """
    # 1) Read header to get bounding box
    p_json = {"pipeline": [{"type": "readers.las", "filename": laz_path}]}
    p = pdal.Pipeline(json.dumps(p_json))
    p.execute()
    md = p.metadata['metadata']['readers.las']
    minx, maxx = md['minx'], md['maxx']
    miny, maxy = md['miny'], md['maxy']
    # centroid in native CRS
    cx, cy = (minx + maxx) / 2.0, (miny + maxy) / 2.0

    # 2) Transform centroid to geographic (EPSG:4326)
    inp_wkt = detect_input_crs_wkt(laz_path)
    transformer = Transformer.from_crs(CRS.from_wkt(inp_wkt), CRS.from_epsg(4326), always_xy=True)
    lon, lat = transformer.transform(cx, cy)

    # 3) Determine UTM zone and EPSG
    zone = int((lon + 180) / 6) + 1
    north = lat >= 0
    epsg_code = 32600 + zone if north else 32700 + zone

    # 4) Read base WKT template
    with open(base_utm_wkt_path, 'r') as f:
        base_wkt = f.read()

    # 5) Modify zone text and central meridian
    zone_str = f"{zone}{'N' if north else 'S'}"
    base_wkt = base_wkt.replace("UTM 10N", f"UTM {zone_str}")
    base_wkt = base_wkt.replace("UTM zone 10N", f"UTM zone {zone_str}")
    # Replace default lon of origin (e.g. -123) with computed
    center_long = find_longitude_of_origin_from_utm(epsg_code)
    base_wkt = base_wkt.replace('"Longitude of natural origin",-123',
                                f'"Longitude of natural origin",{center_long}')

    # 6) Write new WKT to /tmp
    outfn = f"/tmp/UTM_{zone_str}_WGS84_G2139_3D.wkt"
    with open(outfn, 'w') as f:
        f.write(base_wkt)

    return outfn


# # TODO: fix hardcoded local filepaths, they were inserted for testing
# def create_pdal_pipeline(
#     laz_file,
#     aoi,                       # GeoDataFrame/GeoSeries in EPSG:4326 for cropping
#     filter_low_noise=True,     # Exclude class 7
#     filter_high_noise=True,    # Exclude class 18
#     reset_classes=False,       # Force all classes to zero (then SMRF if requested)
#     reclassify_ground=False,   # After reset, SMRF ground reclassification
#     group_filter=None,         # Keep only specific return groups, e.g. "first,only"
#     reproject=True,            # Apply reprojection stage if CRSs differ
#     save_pointcloud=False,     # Write filtered points to LAS/LAZ
#     pointcloud_file="pointcloud",  # Base filename for writer
#     input_crs=None,            # WKT of source CRS
#     output_crs=None,           # WKT or EPSG: target CRS
#     output_type="laz",         # 'laz' or 'las'
#     dsm_percentile=None,       # If set, keep only top X fraction for DSM (e.g. 0.98 for RH98)
#     percentile_filter=True,    # Always run outlier-trimming noise filter
#     percentile_threshold=0.95  # Z-score threshold for noise trimming
# ):
#     """
#     Constructs a PDAL pipeline as a list of stage dicts.

#     Order matters:
#     1. Read and crop to AOI
#     2. Classification-based filtering (reset → SMRF → return groups → noise → road)
#     3. (Optional) ground-only filter
#     4. Reprojection (only if input != output)
#     5. Save intermediate pointcloud (optional)
#     6. Noise-trimming percentile filter (removes extreme Z outliers)
#     7. DSM-percentile filter (retains only top X% returns for canopy DSM)
#     """
#     assert input_crs and output_crs, "CRS arguments required"

#     # Ensure WKT strings if paths provided
#     input_wkt = open(input_crs).read() if os.path.isfile(input_crs) else input_crs
#     output_wkt = open(output_crs).read() if os.path.isfile(output_crs) else output_crs

#     # --------------------------------------------------
#     # 1) Crop AOI: transform AOI to horizontal component of input_crs
#     # --------------------------------------------------

#     # Create a single multipolygon WKT for cropping
#     comp = CRS.from_wkt(input_wkt)
#     horiz = comp.sub_crs_list[0] if comp.is_compound else comp
#     epsg_h = horiz.to_epsg()
#     aoi_proj = aoi.to_crs(epsg_h)
#     poly_wkt = aoi_proj.union_all().wkt

#     pipeline = []
#     pipeline.append({"type": "readers.las", "filename": laz_file})
#     pipeline.append({"type": "filters.crop", "polygon": poly_wkt})

#     # --------------------------------------------------
#     # 2) Classification-based filtering
#     # --------------------------------------------------
#     if reset_classes:
#         # Zero out all classes first
#         pipeline.append({"type": "filters.assign", "value": "Classification = 0"})
#         if reclassify_ground:
#             # Then reclassify ground via SMRF
#             pipeline.append({
#                 "type": "filters.smrf",
#                 "scalar": 1.2,   # height multiplier
#                 "slope": 0.2,    # slope tolerance
#                 "threshold": 0.45,
#                 "window": 8.0
#             })
#     else:
#         # Keep only specific return groups (e.g. first returns only)
#         if group_filter:
#             pipeline.append({"type": "filters.returns", "groups": group_filter})
#         # Remove low-noise and high-noise classes
#         if filter_low_noise:
#             pipeline.append({"type": "filters.range", "limits": "Classification![7:7]"})
#         if filter_high_noise:
#             pipeline.append({"type": "filters.range", "limits": "Classification![18:18]"})
#         # Remove road points if requested
#         # if filter_road:
#         #     pipeline.append({"type": "filters.range", "limits": "Classification![11:11]"})

#     # --------------------------------------------------
#     # 3) Optional ground-only filter
#     # --------------------------------------------------
#     # if return_only_ground:
#     #     pipeline.append({"type": "filters.range", "limits": "Classification[2:2]"})

#     # --------------------------------------------------
#     # 4) Reprojection (must come before writing/outlier DSM filters)
#     # --------------------------------------------------
#     if reproject:
#         in_crs_obj = CRS.from_wkt(input_wkt)
#         out_crs_obj = CRS.from_wkt(output_wkt)
#         if not in_crs_obj.equals(out_crs_obj):
#             pipeline.append({
#                 "type": "filters.reprojection",
#                 "in_srs": input_wkt,
#                 "out_srs": output_wkt
#             })

#     # --------------------------------------------------
#     # 5) Save intermediate pointcloud
#     # --------------------------------------------------
#     if save_pointcloud:
#         writer = {"type": "writers.las", "filename": f"{pointcloud_file}.{output_type}"}
#         if output_type == "laz":
#             # Enable LAZ compression
#             writer.update({
#                 "compression": "true",
#                 "minor_version": "2",
#                 "dataformat_id": "0"
#             })
#         pipeline.append(writer)

#     # --------------------------------------------------
#     # 6) Noise-trimming percentile filter (removes extremes)
#     #    - Should come before DSM-percentile to clean measurement noise
#     # --------------------------------------------------
#     if percentile_filter:
#         pipeline.append({
#             "type": "filters.python",
#             "script": "/home/jehayes/Stereotypical_Helens/filter_percentile.py",
#             "module": "filter_percentile",
#             "function": "filter_percentile",
#             "pdalargs": {"percentile_threshold": percentile_threshold}
#         })

#     # --------------------------------------------------
#     # 7) DSM-percentile filter (keeps only top returns for RH_x DSM)
#     #    - Must come after noise-trimming and reprojection, before rasterization
#     # --------------------------------------------------
#     if dsm_percentile is not None:
#         pipeline.append({
#             "type": "filters.python",
#             "script": "/home/jehayes/Stereotypical_Helens/filter_local_percentile.py",
#             "module": "filter_local_percentile",
#             "function": "filter_local_percentile",
#             "pdalargs": {"percentile_threshold": dsm_percentile} 
#         })

#     # --------------------------------------------------
#     # 8) Add statistical outlier removal to catch residual spikes
#     #    - Placed after reclassification but before DSM filters for speed
#     # --------------------------------------------------
#     pipeline.insert(
#         # After crop and classification filters (position index 3)
#         3,
#         {
#             "type": "filters.outlier",
#             "method": "statistical",
#             "multiplier": 3.0,
#             "mean_k": 8
#         }
#     )

#     return pipeline

def create_pdal_pipeline(
    laz_file,
    aoi,                       # GeoDataFrame/GeoSeries in EPSG:4326
    input_crs=None,            # WKT or path
    output_crs=None,           # WKT or path
    product="dsm",             # 'dsm' for surface, 'dtm' for terrain
    filter_low_noise=True,
    filter_high_noise=True,
    reset_classes=False,
    reclassify_ground=False,
    group_filter=None,
    reproject=True,
    save_pointcloud=False,
    pointcloud_file="pointcloud",
    output_type="laz",
    percentile_filter=True,
    percentile_threshold=0.95,
    dsm_percentile=0.98
):
    """
    DSM vs. DTM pipeline builder.
    - product='dsm': keep all returns, outlier-trim, then canopy RH_x.
    - product='dtm': isolate ground (class 2), outlier-trim only.
    """
    assert input_crs and output_crs
    assert product in ("dsm", "dtm")

    # Load WKT if file paths
    if os.path.isfile(input_crs):
        input_wkt = open(input_crs).read()
    else:
        input_wkt = input_crs
    if os.path.isfile(output_crs):
        output_wkt = open(output_crs).read()
    else:
        output_wkt = output_crs

    # Crop AOI in horizontal CRS
    comp = CRS.from_wkt(input_wkt)
    horiz = comp.sub_crs_list[0] if comp.is_compound else comp
    aoi_m = aoi.to_crs(horiz.to_epsg())
    crop_wkt = aoi_m.unary_union.wkt

    stages = [
        {"type": "readers.las", "filename": laz_file},
        {"type": "filters.crop", "polygon": crop_wkt},
    ]

    # Classification filters
    if reset_classes:
        stages.append({"type": "filters.assign", "value": "Classification = 0"})
        if reclassify_ground:
            stages.append({
                "type": "filters.smrf",
                "scalar": 1.2, "slope": 0.2, "threshold": 0.45, "window": 8.0
            })
    else:
        if group_filter:
            stages.append({"type": "filters.returns", "groups": group_filter})
        if filter_low_noise:
            stages.append({"type": "filters.range", "limits": "Classification![7:7]"})
        if filter_high_noise:
            stages.append({"type": "filters.range", "limits": "Classification![18:18]"})

    # DTM only: keep ground
    if product == "dtm":
        stages.append({"type": "filters.range", "limits": "Classification[2:2]"})

    # Statistical outlier (always)
    stages.append({
        "type": "filters.outlier",
        "method": "statistical",
        "multiplier": 3.0,
        "mean_k": 8
    })

    # Reprojection
    if reproject:
        in_crs_obj = CRS.from_wkt(input_wkt)
        out_crs_obj = CRS.from_wkt(output_wkt)
        if not in_crs_obj.equals(out_crs_obj):
            stages.append({
                "type": "filters.reprojection",
                "in_srs": input_wkt,
                "out_srs": output_wkt
            })

    # Optional save
    if save_pointcloud:
        writer = {"type": "writers.las", "filename": f"{pointcloud_file}.{output_type}"}
        if output_type == "laz":
            writer.update({
                "compression": "true",
                "minor_version": "2",
                "dataformat_id": "0"
            })
        stages.append(writer)

    # Noise‐trim Z outliers
    if percentile_filter:
        stages.append({
            "type": "filters.python",
            "script": "/home/jehayes/Stereotypical_Helens/filter_percentile.py",
            "module": "filter_percentile",
            "function": "filter_percentile",
            "pdalargs": {"percentile_threshold": percentile_threshold}
        })

    # DSM only: canopy‐percentile RH_x
    if product == "dsm":
        stages.append({
            "type": "filters.python",
            "script": "/home/jehayes/Stereotypical_Helens/filter_local_percentile.py",
            "module": "filter_local_percentile",
            "function": "filter_local_percentile",
            "pdalargs": {"percentile_threshold": dsm_percentile}
        })

    return stages

def create_dem_stage(
    dsm_filename="dsm_output.tif",
    pointcloud_resolution=1.0,
    gridmethod="max",    # 'max' yields DSM, 'min' or 'mean' yield DTM, 'idw' interpolates
    dimension="Z",
):
    """
    Defines a GDAL writer stage to generate a geotiff DSM/DTM.
    Compression, tiling, and overview copying are enabled for performance.
    """
    stage = {
        "type": "writers.gdal",
        "filename": dsm_filename,
        "gdaldriver": "GTiff",
        "nodata": -9999,
        "output_type": gridmethod,
        "resolution": float(pointcloud_resolution),
        "gdalopts": (
            "COMPRESS=LZW,TILED=YES,blockxsize=256,blockysize=256,COPY_SRC_OVERVIEWS=YES"
        )
    }
    # Only retain positive Z values for surface models
    if dimension == "Z":
        stage.update({"dimension": "Z", "where": "Z>0"})
    else:
        stage.update({"dimension": dimension})

    return [stage]
