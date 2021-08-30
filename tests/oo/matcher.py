from region import Region
from hierarchy import GeoHierarchy
from operator import lt, le, ge, gt

from rapidfuzz import fuzz, utils
import multiprocessing as mp
import pandas as pd
import math
import re

from scipy import spatial
from ast import literal_eval


class GeoMatcher:

    __slot__ = ("_hierarchy", "_filename", "_chunksize", "_data")

    def __init__(self, hierarchy, filename=""):
        self._hierarchy = hierarchy

        if isinstance(filename, str):
            if filename.strip() == "":
                self._filename = "data/" + hierarchy.name + ".csv"

        else:
            self._filename = filename

        if isinstance(self._filename, str):
            self._data = pd.read_csv(self._filename, dtype="str")
        else:
            self._data = []
            for file in self._filename:
                self._data.append(pd.read_csv(file, dtype="str"))

    # def _perform_address_matching(self, address, df, threshold):
    #    df["RATIO"] = df["FULL_ADRESS"].apply(lambda x: fuzz.ratio(re.sub(r'[\W_]+', '', address.lower()), re.sub(r'[\W_]+', '', x.lower())))
    #    return df[df["RATIO"] >= threshold*100.0]

    def get_region_by_address(
        self,
        address,
        similarity_threshold=0.9,
        top_result=True,
        regions=[],
        operator=None,
        region="",
    ):
        addresses = pd.DataFrame()

        if isinstance(self._data, list):

            for data in self._data:
                data["RATIO"] = data["FULL_ADDRESS"].apply(
                    lambda x: fuzz.ratio(
                        re.sub(r"[\W_]+", "", address.lower()),
                        re.sub(r"[\W_]+", "", x.lower()),
                    )
                )
                if addresses.shape[0] == 0:
                    addresses = data[data["RATIO"] >= similarity_threshold * 100.0]
                else:
                    addresses = addresses.append(
                        data[data["RATIO"] >= similarity_threshold * 100.0],
                        ignore_index=True,
                    )
        else:
            self._data["RATIO"] = self._data["FULL_ADDRESS"].apply(
                lambda x: fuzz.ratio(
                    re.sub(r"[\W_]+", "", address.lower()),
                    re.sub(r"[\W_]+", "", x.lower()),
                )
            )
            addresses = self._data[self._data["RATIO"] >= similarity_threshold * 100.0]

        if addresses.shape[0] > 0:
            if top_result:
                lregions = self._hierarchy.get_regions_by_name(
                    operator=operator, name=region, names=regions
                )
                addresses = addresses.sort_values(by="RATIO", ascending=False)
                for reg in lregions:
                    if reg.col_name != "":
                        print(reg.name + ":" + addresses.loc[0, reg.col_name])
            else:
                lcolumns = self._hierarchy.get_regions_by_name(
                    operator=operator, name=region, names=regions, attribute="col_name"
                )
                print(
                    addresses[list(filter(None, lcolumns)) + ["RATIO"]].sort_values(
                        by="RATIO", ascending=False
                    )
                )

    def cartesian(self, latitude, longitude, elevation=0):

        # Convert to radians
        latitude = latitude * (math.pi / 180)
        longitude = longitude * (math.pi / 180)

        R = 6371  # 6378137.0 + elevation  # relative to centre of the earth
        X = R * math.cos(latitude) * math.cos(longitude)
        Y = R * math.cos(latitude) * math.sin(longitude)
        Z = R * math.sin(latitude)
        return (X, Y, Z)

    def get_region_by_coordinates(self, lat, lon, regions=[], operator=None, region=""):

        places = []
        addresses = pd.DataFrame()
        address = pd.DataFrame()
        # Calculate the catesian coordinates of the input
        cartesian_coord = self.cartesian(lat, lon)

        index = []
        if isinstance(self._data, list):
            for data in self._data:
                data["CARTESIAN_COOR"] = data["CARTESIAN_COOR"].apply(
                    lambda x: literal_eval(str(x))
                )
                places = data["CARTESIAN_COOR"].tolist()
                # Build the tree
                tree = spatial.KDTree(places)
                # Find the nearest point to the input
                closest = tree.query([cartesian_coord], p=2)

                addresses = addresses.append(data.iloc[closest[1][0], :])

        else:
            self._data["CARTESIAN_COOR"] = self._data["CARTESIAN_COOR"].apply(
                lambda x: literal_eval(str(x))
            )
            places = self._data["CARTESIAN_COOR"].tolist()

            # Build the tree
            tree = spatial.KDTree(places)

            # Find the nearest point to the input
            closest = tree.query([cartesian_coord], p=2)

            # Get the index of the first nearest neighbour / point / row
            addresses = addresses.append(self._data.iloc[closest[1][0], :])

        if addresses.shape[0] > 1:
            places = addresses["CARTESIAN_COOR"].tolist()
            # Build the tree
            tree = spatial.KDTree(places)
            # Find the nearest point to the input
            closest = tree.query([cartesian_coord], p=2)

            address = addresses.iloc[closest[1][0], :]

        else:
            address = address.append(addresses.iloc[0])

        lregions = self._hierarchy.get_regions_by_name(
            operator=operator, name=region, names=regions
        )

        for reg in lregions:
            if reg.col_name != "":
                print(reg.name + ":" + address[reg.col_name])
