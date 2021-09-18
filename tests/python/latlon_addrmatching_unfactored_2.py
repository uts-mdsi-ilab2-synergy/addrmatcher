import os
import glob
import time
import numpy as np
from pyarrow import fs
import pyarrow.parquet as pq
from sklearn.neighbors import BallTree

is_index_file_exist = os.path.isfile("../../../data/Master/New/index.parquet")
files = (
    glob.glob(os.path.join("../../../", "data", "Master", "New", "*[0-9].parquet"))
    if is_index_file_exist
    else []
)

## Interested Dimensions in the GNAF Files
interested_dims = [
    "LATITUDE",
    "LONGITUDE",
    "FULL_ADDRESS",
    "STATE",
    "SA4_NAME_2016",
    "LGA_NAME_2016",
    "SSC_NAME_2016",
    "SA3_NAME_2016",
    "SA2_NAME_2016",
    "ADDRESS_DETAIL_PID",
]

local = fs.LocalFileSystem()


# Set Minimum and Maximum lat for all properties within Australia
lat_min = -43.58301104
lat_max = -9.23000371
lon_min = 96.82159219
lon_max = 167.99384663

# 1 lat equals 110.574km
deg = 110.574

# Conversion Rate - radians to kilometer
rad_to_km = 6371


def load_parquet(lat, lon, distance):

    df = pq.read_table(
        files,
        filesystem=local,
        columns=interested_dims,
        filters=[
            ("LATITUDE", ">=", lat - distance),
            ("LATITUDE", "<=", lat + distance),
            ("LONGITUDE", ">=", lon - distance),
            ("LONGITUDE", "<=", lon + distance),
        ],
    ).to_pandas()

    return df


def ensure_lat_lon_within_range(lat, lon):

    # Ensure Latitudge within the AU range
    lat = max(lat, lat_min)
    lat = min(lat, lat_max)

    # Ensure longitutde within the AU range
    lon = max(lon, lon_min)
    lon = min(lon, lon_max)

    return lat, lon


def filter_for_rows_within_mid_distance(df, lat, lon, mid_distance):

    mid_df = df[
        df.LATITUDE.between(lat - mid_distance, lat + mid_distance)
        & df.LONGITUDE.between(lon - mid_distance, lon + mid_distance)
    ]

    return mid_df


def get_region_by_coordinates(lat, lon, km=None, n=1):

    ## 1. Initial distance setting according to lat/lon arguments
    lat, lon = ensure_lat_lon_within_range(lat, lon)
    min_distance = 0
    distance = (km if km else 1) / deg

    ## 2. Make the first load of GNAF dataset
    gnaf_df = load_parquet(lat, lon, distance)

    # 2.a If the desired count of addresses not exist, increase the radius
    while gnaf_df.shape[0] < n:
        min_distance = distance
        distance *= 2

        gnaf_df = load_parquet(lat, lon, distance)
        print("gnaf_df.shape: First Load: ", gnaf_df.shape)

    # 2.b Keep reducing the size of rows if more than 10k adddresses are found within the radius
    # Take the median distance to reduce
    # This is to limit the number of datapoint to build the Ball tree in the next step
    while gnaf_df.shape[0] >= n + 10000:
        middle_distance = (distance - min_distance) / 2
        gnaf_df = filter_for_rows_within_mid_distance(
            gnaf_df, lat, lon, middle_distance
        )
        print("gnaf_df.shape: Reduced Load: ", gnaf_df.shape)
        distance = middle_distance
    print("gnaf_df.shape: Final Load: ", gnaf_df.shape)

    ## 3. Build the Ball Tree and Query for the nearest within k distance
    ball_tree = BallTree(
        np.deg2rad(gnaf_df[["LATITUDE", "LONGITUDE"]].values), metric="haversine"
    )
    distances, indices = ball_tree.query(
        np.deg2rad(np.c_[lat, lon]), k=min(n, gnaf_df.shape[0])
    )
    # Get indices of the search result, Extract pid and calculate distance(km)
    indices = indices[0].tolist()
    pids = gnaf_df.ADDRESS_DETAIL_PID.iloc[indices].tolist()
    distance_map = dict(zip(pids, [distance * rad_to_km for distance in distances[0]]))

    ## 4. Filter the GNAF dataset by address_detail_pid and Extract the interested columns
    bool_list = gnaf_df["ADDRESS_DETAIL_PID"].isin(pids)
    final_gnaf_df = gnaf_df[bool_list]

    final_gnaf_df = final_gnaf_df[interested_dims]
    final_gnaf_df["DISTANCE"] = final_gnaf_df["ADDRESS_DETAIL_PID"].map(distance_map)

    return final_gnaf_df.sort_values("DISTANCE")
