from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import requests
import rasterio
import json
from io import BytesIO
from rasterstats import zonal_stats

app = FastAPI()

# ✅ Request Model
class ZonalStatsRequest(BaseModel):
    wcs_url: str  # URL to fetch GeoTIFF
    shapefile_path: str  # Local path to shapefile
    stats: list = None  # Optional list of stats


# ✅ Function to Fetch and Process Raster Data
def get_multiband_zonal_stats(wcs_url, shapefile_path, stats=None):
    try:
        # Fetch raster from WCS
        response = requests.get(wcs_url)

        if response.status_code != 200:
            raise HTTPException(status_code=500, detail="Failed to retrieve GeoTIFF")

        print("✅ Successfully retrieved GeoTIFF")

        # Default statistics if none provided
        if stats is None:
            stats = ['min', 'max', 'mean', 'median', 'std', 'count']

        # Load raster
        with rasterio.open(BytesIO(response.content)) as src:
            num_bands = src.count  # Get number of bands
            print(f"📌 Raster has {num_bands} bands")

            # Compute statistics for each band
            zonal_statistics = {}

            for band in range(1, num_bands + 1):
                band_stats = zonal_stats(
                    shapefile_path,
                    BytesIO(response.content),
                    band=band,
                    stats=stats
                )
                zonal_statistics[f"Band_{band}"] = band_stats[0]

        return zonal_statistics

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ✅ FastAPI Endpoint
@app.post("/zonal-stats")
def compute_zonal_stats(request: ZonalStatsRequest):
    print("📥 Received WCS URL:", request.wcs_url)
    print("📍 Using shapefile:", request.shapefile_path)

    result = get_multiband_zonal_stats(
        request.wcs_url, request.shapefile_path, request.stats
    )

    print("📊 Computed Stats:", result)
    return {"band_stats": result}