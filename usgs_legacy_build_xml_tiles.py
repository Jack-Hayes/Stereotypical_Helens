#!/usr/bin/env python3
"""
Code to parse a directory of cloud-hosted XML files for USGS ALS legacy data.
I developed this code as I was running into issues where legacy data LAZ file names
did not match up with any of the tile indexes or metadata provided.

This script will:
1. Fetch the list of XML files from a USGS metadata directory.
2. Download each XML in memory (about 11 KB each) in parallel.
3. Extract the bounding box elements (westbc, eastbc, southbc, northbc) from each XML.
4. Output a CSV mapping each LAZ filename to its geographic bounds (4326) in decimal degrees.

Usage:
    python fetch_sthelens_xml_bboxes.py

Test case:
    https://rockyweb.usgs.gov/vdelivery/Datasets/Staged/Elevation/LPC/Projects/legacy/WA_MT_ST_HELENS_2009/metadata/
"""

import sys
import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import pandas as pd

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------
BASE_XML_URL = (
    "https://rockyweb.usgs.gov/"
    "vdelivery/Datasets/Staged/Elevation/LPC/Projects/legacy/"
    "WA_MT_ST_HELENS_2009/metadata/"
)
MAX_WORKERS = 16       # number of parallel threads to fetch XMLs
REQUEST_TIMEOUT = 30   # HTTP request timeout in seconds

# ------------------------------------------------------------------------------
# Helper: list all XML filenames from the metadata directory
# ------------------------------------------------------------------------------
def list_xml_files(base_url: str) -> list[str]:
    """
    Download the HTML index at `base_url` and parse out all links ending in ".xml".
    Return a sorted list of filenames, for example:
      ["USGS_LPC_WA_MT_ST_HELENS_2009_000001.xml", ...].
    """
    try:
        response = requests.get(base_url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
    except Exception as e:
        print(f"[ERROR] Unable to fetch XML directory listing: {e}")
        sys.exit(1)

    soup = BeautifulSoup(response.text, "html.parser")
    xml_files = []
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"].strip()
        if href.lower().endswith(".xml"):
            # Skip parent- or current-directory links like "../"
            if href not in ("../", "./", "/"):
                xml_files.append(href)

    return sorted(set(xml_files))


# ------------------------------------------------------------------------------
# Helper: download and parse a single XML, extract bounding coordinates
# ------------------------------------------------------------------------------
def fetch_xml_bbox(filename: str, base_url: str) -> dict[str, float] | None:
    """
    Download one XML file (about 11 KB) and extract geographic bounds from:
        <spdom>
          <bounding>
            <westbc>... (longitude minimum)</westbc>
            <eastbc>... (longitude maximum)</eastbc>
            <southbc>... (latitude minimum)</southbc>
            <northbc>... (latitude maximum)</northbc>
          </bounding>
        </spdom>
    Returns a dictionary:
      {
        "filename": "<name>.laz",
        "minx": <float(westbc)>,
        "maxx": <float(eastbc)>,
        "miny": <float(southbc)>,
        "maxy": <float(northbc)>
      }
    or returns None if the XML cannot be parsed or fields are missing.
    """
    url = base_url.rstrip("/") + "/" + filename
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
    except Exception as e:
        print(f"[ERROR] {filename}: HTTP error: {e}")
        return None

    try:
        root = ET.fromstring(response.content)
        # Look for <spdom><bounding> ... </bounding></spdom> anywhere in the document
        bounding = root.find(".//spdom/bounding")
        if bounding is None:
            print(f"[WARN] {filename}: <spdom><bounding> element not found.")
            return None

        west = bounding.findtext("westbc")
        east = bounding.findtext("eastbc")
        south = bounding.findtext("southbc")
        north = bounding.findtext("northbc")

        if None in (west, east, south, north):
            print(f"[WARN] {filename}: One of westbc/eastbc/southbc/northbc is missing.")
            return None

        return {
            "filename": filename.replace(".xml", ".laz"),  # match the LAZ tile name
            "minx": float(west),
            "maxx": float(east),
            "miny": float(south),
            "maxy": float(north)
        }

    except ET.ParseError as pe:
        print(f"[ERROR] {filename}: XML parse error: {pe}")
        return None
    except Exception as e:
        print(f"[ERROR] {filename}: Unexpected error: {e}")
        return None


# ------------------------------------------------------------------------------
# Main routine: fetch, parse, and save bounding boxes
# ------------------------------------------------------------------------------
def main():
    print("Fetching list of XML files from the USGS metadata directory...")
    xml_files = list_xml_files(BASE_XML_URL)
    if not xml_files:
        print("No XML files found. Exiting.")
        sys.exit(0)

    print(f"Found {len(xml_files)} XML files.\n")

    results: list[dict[str, float]] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit a task to fetch each XML's bounding box
        future_to_name = {
            executor.submit(fetch_xml_bbox, xml_name, BASE_XML_URL): xml_name
            for xml_name in xml_files
        }

        # As each future completes, collect its result
        for future in tqdm(as_completed(future_to_name), total=len(future_to_name), desc="Processing XMLs"):
            xml_name = future_to_name[future]
            bbox = future.result()
            if bbox is not None:
                results.append(bbox)

    if not results:
        print("Failed to parse any XML bounding boxes. Exiting.")
        sys.exit(1)

    # Build a DataFrame from the results
    df = pd.DataFrame(results)
    df = df.sort_values("filename").reset_index(drop=True)

    # Print a small preview
    print("\nSample of retrieved bounding boxes:")
    print(df.head(10).to_string(index=False, float_format="{:,.6f}".format))

    # Save the results to CSV
    output_csv = "st_helens_2009_tile_bboxes_from_xml.csv"
    df.to_csv(output_csv, index=False)
    print(f"\nDone. Saved results to '{output_csv}'.")

    print("\nEach row in the CSV contains:")
    print("  filename : the .laz tile name (e.g., USGS_LPC_WA_MT_ST_HELENS_2009_000001.laz)")
    print("  minx, maxx : west/east bounds (longitude, decimal degrees)")
    print("  miny, maxy : south/north bounds (latitude, decimal degrees)")
    print("\nYou can now load this CSV into GIS software or reproject coordinates as needed.")


if __name__ == "__main__":
    main()
