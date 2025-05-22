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
import pdal
from pyproj import CRS, Transformer

def detect_input_crs_wkt(laz_path):
    # Define function to read SRS WKT from a .laz via PDAL metadata
    # laz_path: str -> file path to LAZ
    # returns: str WKT
    """
    Reads the spatial reference WKT from a .laz LAS dataset using PDAL metadata.
    Returns a WKT string of the input CRS.
    """
    # Build a minimal PDAL pipeline JSON to open the LAZ file
    pipeline_json = {"pipeline": [{"type": "readers.las", "filename": laz_path}]}
    # type: dict
    pipeline = pdal.Pipeline(json.dumps(pipeline_json))
    # type: pdal.Pipeline
    pipeline.execute()
    # type: executes pipeline, returns count
    # Extract PDAL metadata dict
    md = pipeline.metadata['metadata']['readers.las']
    # type: dict containing reader metadata
    # Try 'spatialreference' key first (WKT string)
    if 'spatialreference' in md and md['spatialreference']:
        # returns string
        return md['spatialreference']
    # If missing, fallback to compound WKT under 'srs'
    if 'srs' in md and 'compoundwkt' in md['srs']:
        # returns string
        return md['srs']['compoundwkt']
    # If none found, raise error
    raise ValueError(f"No valid SRS WKT found in PDAL metadata for: {laz_path}")
    # raises exception if WKT not found


def detect_output_utm_crs_wkt(laz_path):
    # Define function to auto-detect UTM zone 3D CRS WKT
    # laz_path: str -> file path to LAZ
    # returns: str WKT2 compound CRS
    """
    Auto-detects an appropriate 3D UTM zone CRS (with ellipsoidal height) based on the file's centroid,
    using PDAL metadata for bounds. Returns a valid compound CRS WKT (WKT2) string.
    """
    # Build pipeline to read laz
    pipeline_json = {"pipeline": [{"type": "readers.las", "filename": laz_path}]}
    pipeline = pdal.Pipeline(json.dumps(pipeline_json))
    pipeline.execute()
    # Extract bounding box values
    md = pipeline.metadata['metadata']['readers.las']
    minx, maxx = md['minx'], md['maxx']  # floats
    miny, maxy = md['miny'], md['maxy']  # floats
    # Compute centroid in native CRS
    cx, cy = (minx + maxx) / 2.0, (miny + maxy) / 2.0  # floats

    # Transform centroid to geographic CRS
    input_wkt = detect_input_crs_wkt(laz_path)  # str
    transformer = Transformer.from_crs(
        CRS.from_wkt(input_wkt),  # input CRS object
        CRS.from_epsg(4326),       # WGS84 geographic
        always_xy=True
    )  # type: Transformer
    lon, lat = transformer.transform(cx, cy)  # floats in degrees

    # NOTE: could use gpd.estimate_utm_crs() here, but wanted to keep geopandas out of the imports
    # Determine UTM zone and hemisphere
    zone = int((lon + 180) / 6) + 1  # int
    north = lat >= 0  # bool
    # Compute EPSG code for UTM on WGS84
    epsg_code = 32600 + zone if north else 32700 + zone  # int
    utm_2d = CRS.from_epsg(epsg_code)  # CRS object
    # Elevate to 3D compound CRS with ellipsoidal height
    name = f"{utm_2d.name} + Ellipsoidal Height"  # str
    utm_3d = utm_2d.to_3d(name=name)  # CRS object
    # Return default WKT2 compound string
    return utm_3d.to_wkt()  # str


def create_pdal_pipeline(
    laz_file,
    aoi,  # GeoDataFrame or GeoSeries in EPSG:4326
    filter_low_noise=True,
    filter_high_noise=True,
    filter_road=True,
    reset_classes=False,
    reclassify_ground=False,
    return_only_ground=False,
    group_filter=None,
    reproject=True,
    save_pointcloud=False,
    pointcloud_file="pointcloud",
    input_crs=None,
    output_crs=None,
    output_type="laz",
    percentile_filter=True,
    percentile_threshold=0.95,
):
    # Define pipeline builder with AOI cropping and filtering
    # laz_file: str path
    # aoi: GeoDataFrame/Series
    # returns: list of stage dicts
    assert input_crs, "input_crs must be provided"
    assert output_crs, "output_crs must be provided"
    # Convert AOI from EPSG:4326 to horizontal component of input_crs
    if hasattr(aoi, 'to_crs'):
        compound_crs = CRS.from_wkt(input_crs)  # CRS
        if compound_crs.is_compound:
            horizontal_crs = compound_crs.sub_crs_list[0]  # CRS
            horizontal_epsg = horizontal_crs.to_epsg()  # int or None
        else:
            horizontal_crs = compound_crs
            horizontal_epsg = horizontal_crs.to_epsg()  # int or None
        aoi_proj = aoi.to_crs(horizontal_epsg)  # GeoDataFrame/Series
    else:
        raise ValueError("AOI must be a GeoDataFrame or GeoSeries with to_crs method")
    poly_wkt = aoi_proj.union_all().wkt  # str WKT of polygon

    pipeline = []  # list to collect PDAL stages
    # Stage: read the LAZ file
    pipeline.append({"type": "readers.las", "filename": laz_file})
    # Stage: crop to AOI polygon
    pipeline.append({"type": "filters.crop", "polygon": poly_wkt})

    # Classification filtering logic
    if reset_classes:
        # Reset all classifications to 0
        pipeline.append({"type": "filters.assign", "value": "Classification = 0"})
        if reclassify_ground:
            # SMRF ground reclassification
            pipeline.append({
                "type": "filters.smrf", "scalar": 1.2, "slope": 0.2,
                "threshold": 0.45, "window": 8.0
            })
    else:
        if group_filter:
            # Keep only specified return groups
            pipeline.append({"type": "filters.returns", "groups": group_filter})
        if filter_low_noise:
            # Exclude low-noise classification 7
            pipeline.append({"type": "filters.range", "limits": "Classification![7:7]"})
        if filter_high_noise:
            # Exclude high-noise classification 18
            pipeline.append({"type": "filters.range", "limits": "Classification![18:18]"})
        if filter_road:
            # Exclude road classification 11
            pipeline.append({"type": "filters.range", "limits": "Classification![11:11]"})

    if return_only_ground:
        # Keep only ground points (class 2)
        pipeline.append({"type": "filters.range", "limits": "Classification[2:2]"})

    # Reprojection stage if needed
    if reproject:
        in_crs_obj = CRS.from_wkt(input_crs)  # CRS
        out_crs_obj = CRS.from_wkt(output_crs)  # CRS
        if not in_crs_obj.equals(out_crs_obj):
            pipeline.append({
                "type": "filters.reprojection",
                "in_srs": input_crs,
                "out_srs": output_crs
            })

    # Optional saving of filtered point cloud
    if save_pointcloud:
        writer = {"type": "writers.las", "filename": f"{pointcloud_file}.{output_type}"}
        if output_type == "laz":
            # LAZ writer with compression
            writer.update({
                "compression": "true",
                "minor_version": "2",
                "dataformat_id": "0"
            })
        pipeline.append(writer)

    # Percentile-based outlier filter must run last
    if percentile_filter:
        pipeline.append({
            "type": "filters.python",
            "script": "filter_percentile.py",
            "pdalargs": {"percentile_threshold": percentile_threshold},
            "function": "filter_percentile",
            "module": "anything"
        })

    return pipeline  # list of dict stages


def create_dsm_stage(
    dsm_filename="dsm_output.tif",
    pointcloud_resolution=1.0,
    gridmethod="idw",
    dimension="Z",
):
    # Build a GDAL writer stage dict for generating a DSM raster
    dsm_stage = {
        "type": "writers.gdal",                # GDAL writer driver
        "filename": dsm_filename,               # output filename
        "gdaldriver": "GTiff",                # driver name
        "nodata": -9999,                        # no-data value
        "output_type": gridmethod,              # interpolation method
        "resolution": float(pointcloud_resolution),  # cell size
        "gdalopts": "COMPRESS=LZW,TILED=YES,blockxsize=256,blockysize=256,COPY_SRC_OVERVIEWS=YES"
    }
    if dimension == "Z":
        # Only include positive height values
        dsm_stage.update({"dimension": "Z", "where": "Z>0"})
    else:
        # Use other specified dimension
        dsm_stage.update({"dimension": dimension})
    return [dsm_stage]  # returns list containing a single dict
