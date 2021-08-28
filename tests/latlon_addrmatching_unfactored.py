import math
import pandas as pd
from scipy import spatial
from ast import literal_eval

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
places = []

######### Get the catesian coordinates of all addresses in GNAF dataset ######
# Convert the string type '(xxx,xxx,xxx)' to tuple (xxx,xxx,xxx)
gnaf_address_combined["CARTESIAN_COOR"] = gnaf_address_combined["CARTESIAN_COOR"].apply(
    lambda x: literal_eval(str(x))
)
# Convert the column value to a list which will be used to construct a KDTree in next step
places = gnaf_address_combined["CARTESIAN_COOR"].tolist()


def cartesian(latitude, longitude, elevation=0):

    # Convert to radians
    latitude = latitude * (math.pi / 180)
    longitude = longitude * (math.pi / 180)

    R = 6371  # 6378137.0 + elevation  # relative to centre of the earth
    X = R * math.cos(latitude) * math.cos(longitude)
    Y = R * math.cos(latitude) * math.sin(longitude)
    Z = R * math.sin(latitude)
    return (X, Y, Z)


def getRegionByCoordinates(lat, lon):

    # Calculate the catesian coordinates of the input
    cartesian_coord = cartesian(lat, lon)

    # Build the tree
    tree = spatial.KDTree(places)

    # Find the nearest point to the input
    closest = tree.query([cartesian_coord], p=2)

    # Get the index of the first closest point / row
    index = closest[1][0]

    ## Get the result row
    res = gnaf_address_combined.iloc[index, :]
    print(res["FULL_ADDRESS"])

    # Get the Meshed block code of the record to find the match in ASGS dataset
    mb = res["MB_2016_PID"]
    print(mb)

    # Remove the (prefix)first 4 characters from the string suuch as MB1680024002000 in GNAF , but 80024002000 in ASGS
    # Get the matching record in ASGS
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
