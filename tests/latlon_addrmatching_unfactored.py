import pandas as pd
import math
from scipy import spatial

gnaf_address_combined = pd.read_csv("AU-GNAF.csv")
asgs = pd.read_csv("AU-ASGS.csv", dtype="str")


AU = {}
AU["MESH_BLOCK"] = "MB_CODE_2016"
AU["SA1"] = "SA1_7DIGITCODE_2016"
AU["SA2"] = "SA2_NAME_2016"
AU["SA3"] = "SA3_NAME_2016"
AU["SA4"] = "SA4_NAME_2016"
AU["SUBURB"] = "SSC_NAME_2016"
AU["LGA"] = "LGA_NAME_2016"
AU["STATE"] = "STATE_NAME_2016"

country = AU


def cartesian(latitude, longitude, elevation=0):
    # Convert to radians
    latitude = latitude * (math.pi / 180)
    longitude = longitude * (math.pi / 180)

    R = 6371  # 6378137.0 + elevation  # relative to centre of the earth
    X = R * math.cos(latitude) * math.cos(longitude)
    Y = R * math.cos(latitude) * math.sin(longitude)
    Z = R * math.sin(latitude)
    return (X, Y, Z)


def getCartesianCoordinatesArray():
    places = []
    for index, row in gnaf_address_combined[["CARTESIAN_COOR"]].iterrows():
        places.append(row["CARTESIAN_COOR"])
    return places


def getRegionByCoordinates(lat, lon):

    cartesian_coord = cartesian(lat, lon)

    ### Actually CARTESIAN_COOR is already saved in Dataset
    gnaf_address_combined["CARTESIAN_COOR"] = gnaf_address_combined[
        ["LATITUDE", "LONGITUDE"]
    ].apply(lambda x: cartesian(*list((x.LATITUDE, x.LONGITUDE))), axis=1)
    places = getCartesianCoordinatesArray()

    tree = spatial.KDTree(places)
    closest = tree.query([cartesian_coord], p=2)
    index = closest[1][0]

    ## Get the result row
    res = gnaf_address_combined.iloc[index, :]
    print(res["FULL_ADDRESS"])

    mb = res["MB_2016_PID"]
    print(mb)
    region = asgs[asgs["MB_CODE_2016"] == mb[4:]]

    for key, value in AU.items():
        print(key + ":" + region.iloc[0][value])


#### Testing the methods
lat = -35.19678041
lon = 149.02779517
getRegionByCoordinates(lat, lon)
#### Output
# 11 SWALLOW STREET, DUNLOP, ACT 2615
# MB1680011580000
# MESH_BLOCK:80011580000
# SA1:8100604
# SA2:Dunlop
# SA3:Belconnen
# SA4:Australian Capital Territory
# SUBURB:Dunlop
# LGA:Unincorporated ACT
# STATE:Australian Capital Territory
