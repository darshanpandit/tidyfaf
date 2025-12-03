import os
import subprocess
import zipfile
from pathlib import Path

def download_file(url, dest_path):
    print(f"Downloading {url} to {dest_path}...")
    import urllib.request
    try:
        with urllib.request.urlopen(url) as response, open(dest_path, 'wb') as out_file:
            shutil.copyfileobj(response, out_file)
        print("Download complete.")
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        raise

def extract_zip(zip_path, extract_to):
    print(f"Extracting {zip_path} to {extract_to}...")
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
            return zip_ref.namelist()
        print("Extraction complete.")
    except Exception as e:
        print(f"Error extracting {zip_path}: {e}")
        raise

import pandas as pd
import geopandas as gpd
import pyogrio
import shutil

def download_and_process():
    # Define paths
    data_dir = Path.home() / ".tidyfaf_data"
    
    # Create data directory if it doesn't exist
    data_dir.mkdir(parents=True, exist_ok=True)
    
    # Define datasets
    datasets = [
        {
            "name": "FAF5 Regional Database",
            "url": "https://faf.ornl.gov/faf5/Data/Download_Files/FAF5.7.1.zip",
            "filename": "FAF5.7.1.zip",
            "csv_name": "FAF5.7.1.csv"
        },
        {
            "name": "FAF5 HiLo Forecasts",
            "url": "https://faf.ornl.gov/faf5/Data/Download_Files/FAF5.7.1_HiLoForecasts.zip",
            "filename": "FAF5.7.1_HiLoForecasts.zip",
            "csv_name": "FAF5.7.1_HiLoForecasts.csv"
        },
        {
            "name": "FAF5 State Database",
            "url": "https://faf.ornl.gov/faf5/data/FAF5.7.1_State.zip",
        "filename": "FAF5.7.1_State.zip",
        "csv_name": "FAF5.7.1_State.csv"
    },
    {
        "name": "FAF5 State HiLo Forecasts",
        "url": "https://faf.ornl.gov/faf5/Data/Download_Files/FAF5.7.1_State_HiLoForecasts.zip",
        "filename": "FAF5.7.1_State_HiLoForecasts.zip",
        "csv_name": "FAF5.7.1_State_HiLoForecasts.csv"
    },
    {
        "name": "FAF5 Zones Shapefile",
        "url": "https://www2.census.gov/programs-surveys/cfs/technical-documentation/geographies/Shapefile%20of%20CFS%20Metro%20Areas%20for%202017%20(requires%20ArcGIS%20to%20Open).zip",
        "filename": "FAF5_Zones.zip",
        "csv_name": None # It's a shapefile
    },
    {
        "name": "FAF5 Network Database",
            "url": "https://ops.fhwa.dot.gov/freight/freight_analysis/faf/faf_highway_assignment_results/FAF5_Model_Highway_Network.zip",
            "filename": "FAF5_Model_Highway_Network.zip",
            "csv_name": None # Network data is GDB/Shapefile, not CSV to convert
        }
    ]
    
    for dataset in datasets:
        file_path = data_dir / dataset["filename"]
        parquet_path = None
        if dataset["csv_name"]:
             parquet_path = data_dir / Path(dataset["csv_name"]).with_suffix(".parquet")

        # Check if final output exists
        if parquet_path and parquet_path.exists():
             print(f"{parquet_path.name} already exists. Skipping.")
             continue
        elif dataset["name"] == "FAF5 Network Database" and (data_dir / "FAF5_Network_Links.parquet").exists():
             print("Network GeoParquet already exists. Skipping.")
             continue
        elif dataset["name"] == "FAF5 Zones Shapefile" and (data_dir / "FAF5_Zones.parquet").exists():
             print("FAF5 Zones GeoParquet already exists. Skipping.")
             continue

        # Check if we need to download
        need_download = True
        if file_path.exists():
            print(f"{dataset['filename']} already exists. Skipping download.")
            need_download = False
        elif dataset["name"] == "FAF5 Network Database" and (data_dir / "Networks").exists():
             print("Network directory likely exists. Skipping download.")
             need_download = False
        elif dataset["name"] == "FAF5 Zones Shapefile" and any(data_dir.rglob("*.shp")): # Check if any shp exists
             print("Shapefile directory likely exists. Skipping download.")
             need_download = False

        extracted_files = []
        if need_download:
            download_file(dataset["url"], file_path)
            extracted_files = extract_zip(file_path, data_dir)
            print(f"Removing {file_path}...")
            file_path.unlink()
            print("Zip removal complete.")

        # Convert to Parquet if applicable
        if dataset["csv_name"]:
            csv_path = data_dir / dataset["csv_name"]
            if csv_path.exists():
                print(f"Converting {csv_path.name} to Parquet...")
                try:
                    df = pd.read_csv(csv_path, low_memory=False)
                    df.to_parquet(parquet_path)
                    print(f"Saved to {parquet_path.name}")
                    
                    print(f"Removing original CSV {csv_path.name}...")
                    csv_path.unlink()
                    print("CSV removal complete.")
                except Exception as e:
                    print(f"Error converting {csv_path.name}: {e}")
            else:
                print(f"Warning: Expected CSV {dataset['csv_name']} not found after extraction.")
        
        # Convert Network GDB to GeoParquet
        if dataset["name"] == "FAF5 Network Database":
            network_dir = data_dir / "Networks"
            gdb_path = network_dir / "Geodatabase Format" / "FAF5Network.gdb"
            output_parquet = data_dir / "FAF5_Network.parquet"
            
            if gdb_path.exists():
                print(f"Converting {gdb_path} to GeoParquet...")
                try:
                    # List all layers
                    layers = pyogrio.list_layers(gdb_path)
                    print(f"Found layers: {layers[:, 0]}")
                    
                    for layer_name in layers[:, 0]:
                        output_name = f"FAF5_Network_{layer_name.replace('FAF5_', '')}.parquet"
                        output_parquet = data_dir / output_name
                        
                        print(f"Converting layer {layer_name} to {output_name}...")
                        gdf = gpd.read_file(gdb_path, layer=layer_name)
                        gdf.to_parquet(output_parquet)
                        print(f"Saved to {output_parquet.name}")

                except Exception as e:
                    print(f"Error converting GDB: {e}")
            else:
                print(f"GDB not found at {gdb_path}")

        elif dataset["name"] == "FAF5 Zones Shapefile":
            # Convert Shapefile to Parquet
            # The zip contains a folder likely named "Shapefile of CFS Metro Areas for 2017 (requires ArcGIS to Open)"
            # We need to find the .shp file inside.
            
            output_parquet = data_dir / "FAF5_Zones.parquet"
            if output_parquet.exists():
                print(f"{output_parquet.name} already exists. Skipping.")
                continue
                
            print("Searching for shapefile...")
            shp_files = list(data_dir.rglob("*.shp"))
            # Filter for the one we just extracted if multiple exist
            # The zip name is FAF5_Zones.zip, but extracted folder name is long.
            # Let's look for 'CFS_Areas' or similar in the name if possible, or just take the first one found in the extracted dir.
            # Since we extract to data_dir, and we just extracted it.
            
            target_shp = None
            for shp in shp_files:
                if "CFS" in shp.name or "Area" in shp.name:
                    target_shp = shp
                    break
            
            if target_shp:
                print(f"Converting {target_shp} to GeoParquet...")
                try:
                    gdf = gpd.read_file(target_shp)
                    # Ensure CRS is 4326 for web mapping
                    if gdf.crs != "EPSG:4326":
                        gdf = gdf.to_crs("EPSG:4326")
                    gdf.to_parquet(output_parquet)
                    print(f"Saved to {output_parquet.name}")
                    
                    # Cleanup extracted files if they were downloaded
                    if extracted_files:
                        print("Cleaning up extracted shapefiles...")
                        for f_name in extracted_files:
                            f_path = data_dir / f_name
                            if f_path.exists() and f_path.is_file():
                                try:
                                    f_path.unlink()
                                except Exception:
                                    pass
                        print("Cleanup complete.")
                    elif target_shp:
                        # Fallback: If we didn't download (files existed), but we converted,
                        # we should clean up the specific shapefile we used and its siblings.
                        print(f"Cleaning up converted shapefile {target_shp.name} and siblings...")
                        base_name = target_shp.stem
                        # Use target_shp.parent to ensure we look in the correct directory (e.g. if inside a subdir)
                        for f in target_shp.parent.glob(f"{base_name}.*"):
                            try:
                                f.unlink()
                            except Exception:
                                pass
                        print("Cleanup complete.")
                except Exception as e:
                    print(f"Error converting Shapefile: {e}")
            else:
                print("No suitable shapefile found after extraction.")

if __name__ == "__main__":
    download_and_process()
