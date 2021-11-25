from rapidfuzz import fuzz
from rapidfuzz.string_metric import jaro_similarity, jaro_winkler_similarity
import pandas as pd
import numpy as np
import re
import os
import glob
from pyarrow import fs
import pyarrow.parquet as pq
from sklearn.neighbors import BallTree
from enum import Enum


class DistanceMethod(Enum):
    LEVENSHTEIN = 1
    JARO = 2
    JARO_WINKLER = 3    

class GeoMatcher:

    __slots__ = (
        "_hierarchy",
        "_file_location",
        "_index_data",
        "_filenames",
        "_street_code_dict",
    )

    def __init__(self, hierarchy, file_location=""):
        self._hierarchy = hierarchy

        # if no file location provided, look for the dataset in the default folder: data/[country]
        if not file_location.strip():
            if os.path.isdir(os.path.join("data", self._hierarchy.name)):
                self._file_location = os.path.join("data", self._hierarchy.name)
            else:
                raise ValueError(
                    f"Folder names that contain the index file can't be found: "
                    f"{os.path.join('data', self._hierarchy.name)}"
                )
        else:
            if os.path.isdir(file_location):
                self._file_location = file_location
            else:
                raise ValueError(
                    f"Folder names that contain the index file can't be found: "
                    f"{file_location}"
                )

        # get all the parquet filenames within the folder
        self._filenames = glob.glob(os.path.join(self._file_location, "*.parquet"))

        # init
        index_file = "index.parquet"

        # check if the index file exists
        if os.path.join(self._file_location, index_file) not in self._filenames:
            raise ValueError(
                f"Index file ({index_file}) can't be found in: {self._file_location}"
            )

        # read the index file
        self._index_data = pd.read_parquet(
            os.path.join(self._file_location, index_file)
        )

        # check the availability of required column name
        idx_columns = ["IDX", "ADDRESS", "FILE_NAME"]
        if not set(idx_columns).issubset(self._index_data.columns):
            raise ValueError(
                f"The required columns can't be found in the index file: "
                f"{str(set(idx_columns) - set(self._index_data.columns))}"
            )

        # remove index file from the lists
        self._filenames.remove(os.path.join(self._file_location, index_file))

        # check parquet file schema (ensure all of the required columns are exist)
        # get the regions that users selected
        all_regions = self._hierarchy.get_regions_by_name(attribute="col_name")
        all_columns = list(filter(None, all_regions))

        for file in self._filenames:
            pq_columns = pq.read_schema(file).names
            if not set(all_columns).issubset(pq_columns):
                raise ValueError(
                    f"The required columns {str(set(all_columns) - set(pq_columns))}"
                    f" can't be found in the parquet file: {file}"
                )

        # define the dictionary for street code normalization
        self._street_code_dict = {
            "ALLY": "ALLEY",
            "ALY": "ALLEY",
            "ARC": "ARCADE",
            "AVE": "AVENUE",
            "AV": "AVENUE",
            "BLTWY": "BELTWAY",
            "BVD": "BOULEVARD",
            "BYPA": "BYPASS",
            "CCT": "CIRCUIT",
            "CL": "CLOSE",
            "CRN": "CORNER",
            "CT": "COURT",
            "CRES": "CRESCENT",
            "CSWY": "CAUSEWAY",
            "CDS": "CUL-DE-SAC",
            "DR": "DRIVE",
            "ESP": "ESPLANADE",
            "GRN": "GREEN",
            "GR": "GROVE",
            "HWY": "HIGHWAY",
            "JNC": "JUNCTION",
            "LN": "LANE",
            "LANE": "LANE",
            "LINK": "LINK",
            "MEWS": "MEWS",
            "PDE": "PARADE",
            "PKWY": "PARKWAY",
            "PL": "PLACE",
            "RDGE": "RIDGE",
            "RD": "ROAD",
            "SQ": "SQUARE",
            "ST": "STREET",
            "TCE": "TERRACE",
            "TPKE": "TURNPIKE",
            "WAY": "WAY",
        }

    def _remove_street_number(self, address):
        """
        Remove the street number, lot/unit/level number, or similar attributes
        from the address
        
        Parameter
        ---------
        address: string
            The physical address' text
        
        Return
        ------
            An address without the street number
        """

        # initiate the result
        no_number_address = address
        while True:
            match = re.search(r"[0-9]+[a-zA-Z,]*\s", address)
            if not match:
                break

            pos = match.span()[1]
            address = address[pos:]
            no_number_address = address.upper()

        # change to upper case
        no_number_address = no_number_address.replace(",", "").strip().upper()
        # replace the street suffix abbreviation with the street suffix name
        # and remove the extra spaces
        address_parts = no_number_address.split()

        if address_parts[0] == "ST":
            address_parts.pop(0)
            return "ST " + " ".join(
                [self._street_code_dict.get(item, item) for item in address_parts]
            )
        else:
            return " ".join(
                [self._street_code_dict.get(item, item) for item in address_parts]
            )

    def _cleaning_match_with_index(self, no_number_address):
        """
        Return similar addresses of the no_number_address with
        the index file based on postcode and state or
        locality/suburb or street name.
        
        Parameter
        ---------
        no_number_address: string
            The physical address without the street number,
            lot/unit/level number, or similar attribute
        
        Returns
        -------
            A matched address
        """

        # initiate the dataframe
        matched_df = pd.DataFrame()

        # search State and Postcode and extract - example QLD 410
        match = re.search(
            r"\s((?:NSW|VIC|QLD|TAS|WA|SA|NT|ACT))\s([0-9]{4})$", no_number_address
        )
        if not match:
            return matched_df

        state, postcode = match.group(1), match.group(2)

        # firstly, Filter for the rows from the Index File based on same Postcode and State
        matched_postcode_state = self._index_data[
            (self._index_data["POSTCODE"] == postcode)
            & (self._index_data["STATE"] == state)
        ]
        if matched_postcode_state.empty:
            return matched_df

        # secondly, Filter further for same Suburbs,
        # get the Distinct Suburb Name List first - "LOCALITY_NAME"

        suburb_list = matched_postcode_state["LOCALITY_NAME"].unique()
        suburbs = [sub for sub in suburb_list if sub in no_number_address]
        if not suburbs:
            return matched_df

        # get the boolean array - of rows matching (LOCALITY_NAME)
        # with filtered suburbs in the above list
        index = matched_postcode_state["LOCALITY_NAME"].str.contains("|".join(suburbs))
        if not any(index):
            return matched_df

        # find Street and Street code from no_number_address to further matching by street name
        # sample match output - <re.Match object; span=(29, 46), match='WEST END QLD 4101'>
        match = re.search(
            f"(?:{'|'.join(suburbs)})\s+{state}\s+{postcode}", no_number_address
        )
        if not match:
            return matched_df
        # extract (unit, street) from the whole address string
        # - UNIT 415 21 ABC STREET  from "UNIT 415 21 ABC STREET WEST END QLD 4101"
        street_string = no_number_address[: match.span()[0]]

        # thirdly, Filter further by Street Name within the filtered Suburb
        subb_df = matched_postcode_state[index]
        unique_street_name_list = subb_df["STREET_NAME"].unique()
        street_name = [
            street_name
            for street_name in unique_street_name_list
            if street_name in street_string
        ]

        if (
            street_name
        ):  # if street name does not exist in the data frame, suburbs maybe incorrect
            matched_df = subb_df[subb_df["STREET_NAME"].isin(street_name)]

        else:
            # find within the same post code and state again for street name
            street_name = [
                street_name
                for street_name in unique_street_name_list
                if street_name in street_string
            ]
            matched_df = matched_postcode_state[
                matched_postcode_state["STREET_NAME"].isin(street_name)
            ]

        return matched_df

    def _cleaning_address(self, no_number_address):
        """
        Return the clean address.
        The function will revise the locality/suburb or other attributes
        if necessary.
        
        Parameter
        ---------
        no_number_address: string
            The physical address without the street number,
            lot/unit/level number, or similar attribute
        
        Return
        ------
            A clean address
        """

        matched_df = self._cleaning_match_with_index(no_number_address)

        if matched_df.empty:
            return no_number_address
        else:

            match = re.search(
                "\s((?:NSW|VIC|QLD|TAS|WA|SA|NT|ACT))\s([0-9]{4})$", no_number_address
            )
            if match:
                state, postcode = match.group(1), match.group(2)

            match = re.search(
                f"(?:({'|'.join(matched_df['STREET_NAME'].unique())}))\s(.*)\s((:?{'|'.join(matched_df['LOCALITY_NAME'].unique())}))",
                no_number_address,
            )

            if not match:
                match = re.search(
                    f"(?:({'|'.join(matched_df['STREET_NAME'].unique())}))\s(\S*)\s",
                    no_number_address,
                )
                if not match:
                    return no_number_address

                street_name = match.group(1)
                street_code = match.group(2)

                match = re.search(f"{street_code}\s(.+)\s{state}", no_number_address)
                # Suburb maybe incorrectly entered
                suburb = match.group(1) if match else None
            else:
                street_name, street_code, suburb = (
                    match.group(i + 1) for i in range(3)
                )

            if suburb:
                # Get the index of filter based on Street and Suburb name
                index = (matched_df["STREET_NAME"] == street_name) & (
                    matched_df["LOCALITY_NAME"] == suburb
                )
                # If no exact match found, try to find matching string with Street name and Suburb
                index = (
                    index
                    if any(index)
                    else (matched_df["STREET_NAME"].str.contains(street_name))
                    & (matched_df["LOCALITY_NAME"].str.contains(suburb))
                )

                # if there is no matching suburbs found, use street name only to filter
                matched_df = (
                    matched_df[index]
                    if any(index)
                    else matched_df[
                        (matched_df["STREET_NAME"].str.contains(street_name))
                    ]
                )

            else:
                matched_df = matched_df[(matched_df["STREET_NAME"] == street_name)]

            if matched_df.empty:
                return no_number_address
            if matched_df.shape[0] == 1:
                return matched_df["ADDRESS"].iloc[0]

            # Covert street code to long form postcode
            street_code = (
                self._street_code_dict[street_code]
                if street_code in self._street_code_dict
                else street_code
            )
            matched_df = matched_df[matched_df["STREET_TYPE_CODE"] == street_code]

            if matched_df.empty:
                return no_number_address
            if matched_df.shape[0] == 1:
                return matched_df["ADDRESS"].iloc[0]
            return no_number_address

    def get_region_by_address(
        self,
        address,
        similarity_threshold=0.9,
        nlargest=1,
        regions=None,
        operator=None,
        address_cleaning=False,
        method=DistanceMethod.LEVENSHTEIN,
    ):
        """
        perform address based matching and return the corresponding region
        e.g. administrative level or statistical area

        Parameters
        ----------
        address:string
            The complete physical address
        similarity_threshold:float
            The minimum similarity ratio ranges between 0 and 1 (default = 0.9)
        nlargest:int
            The number of the addresses to be returned by the function.
            If nlargest = 1, then the function will return the top similarity only
            (default = 1)
        regions:string or list of string
            Specify the name or list of names of the regions to be returned by the function
        operator: Operator
            use the operator (Operator.ge or Operator.le) to find all the 
            upper/lower level regions from a particular region name.
            For instance: Country >= State (Country ge State).
                          Use the 'ge' operator to search for the upper level of State
                          (and itself)
        address_cleaning:boolean
            whether to perform data cleansing on the address, 
            for instance: revise invalid suburb name 
            (currently, only applied to Australian addresses. Set this parameter 
            as True for non-Australian addresses could return an error)
        method:string
            The name of the edit distance algorithm used.
            Select one of DistanceMethod.LEVENSHTEIN,DistanceMethod.JARO, 
            or DistanceMethod.JARO_WINKLER
        
        Returns
        -------
        Dictionary
            the dictionary of the matched adddresses. The keys of the dictionary 
            are based on the column name defined in the Hierarchy object used. 
            By default, the function will return only the top similarity record 
            (nlargest = 1) as long as its similarity is larger than the threshold 
            ratio. If no addresses have a similarity ratio more than the 
            threshold, the function will return an empty dictionary.
        
        Examples
        --------
        >>> matcher = GeoMatcher(AUS)
        >>> matched = matcher.get_region_by_address("2885 Darnley Street,
                      Braybrookt, VIC 3019", similarity_threshold = 0.95)
        >>> matched
        {'MB_CODE_2016': ['20375120000'],
         'SA4_NAME_2016': ['Melbourne - West'],
         'SA3_NAME_2016': ['Maribyrnong'],
         'SA2_NAME_2016': ['Braybrook']
         'SA1_7DIGITCODE_2016': ['2134703'],
         'STATE': ['VIC'],
         'RATIO': [0.9841269841269842],
         'SSC_NAME_2016': ['Braybrook'],
         'LGA_NAME_2016': ['Maribyrnong (C)'],
         'FULL_ADDRESS': ['2885 DARNLEY STREET BRAYBROOK VIC 3019']}
        """

        if not isinstance(method, DistanceMethod):
            raise ValueError(
                f"String metric is unknown. Select one of {[e.value for e in DistanceMethod]}"
            )

        if similarity_threshold < 0:
            raise ValueError("Similarity threshold has to be larger than 0")

        if nlargest < 1:
            raise ValueError("The number of returned records must be at least 1")
            
        #list of functions
        dist_functions = {DistanceMethod.LEVENSHTEIN: fuzz.ratio,
                          DistanceMethod.JARO: jaro_similarity,
                          DistanceMethod.JARO_WINKLER: jaro_winkler_similarity}

        # initiate the result
        addresses = pd.DataFrame()

        clean_address = self._remove_street_number(address)

        if address_cleaning:
            # perform further cleaning
            clean_address = self._cleaning_address(clean_address)

        # print(clean_address)

        # match with the index
        if (self._index_data is not None) and (self._index_data.shape[0] > 0):
            # no clean address found
            clean_address_idx = self._index_data[
                self._index_data["ADDRESS"] == clean_address
            ]

            if clean_address_idx.shape[0] == 0:
                # calculate the distance (Levenshtein Distance) between the
                # input address (without street number) and the index
                # [all special characters are removed]
                
                self._index_data["RATIO"] = self._index_data["ADDRESS"].apply(
                    lambda x: dist_functions[method](
                        re.sub(r"[\W_]+", "", clean_address),
                        re.sub(r"[\W_]+", "", x.upper()),
                    )
                    / 100.0
                )

                # get the index with the largest similarity
                largest_idx = self._index_data.nlargest(1, "RATIO")

                if largest_idx["RATIO"].values[0] < similarity_threshold:
                    return {}

                # first, check the filename it it's available
                parquet_filename = largest_idx["FILE_NAME"].values[0]
                parquet_idx = largest_idx["IDX"].values[0]
            else:

                parquet_filename = clean_address_idx["FILE_NAME"].values[0]
                parquet_idx = clean_address_idx["IDX"].values[0]

            if os.path.isfile(os.path.join(self._file_location, parquet_filename)):
                # read the parquet file where the IDX and address are stored
                address_parquet = pq.read_table(
                    os.path.join(self._file_location, parquet_filename),
                    filesystem=fs.LocalFileSystem(),
                    filters=[("IDX", "=", parquet_idx)],
                ).to_pandas()

                # calculate the distance (Levenshtein Distance) between the
                # input address (with street number) and the entire addresses
                # reference dataset
                # [all special characters are removed]
                
                address_parquet["RATIO"] = address_parquet["FULL_ADDRESS"].apply(
                    lambda x: dist_functions[method](
                        re.sub(r"[\W_]+", "", address.upper()),
                        re.sub(r"[\W_]+", "", x.upper()),
                    )
                    / 100.0
                )

                # if similarity score is larger then the threshold,
                # there is a possibility the addresses are similar
                # will need to select the highest score later on
                addresses = address_parquet[
                    address_parquet["RATIO"] >= similarity_threshold
                ]

                # get the regions that users selected
                selected_regions = self._hierarchy.get_regions_by_name(
                    region_names = regions, operator=operator
                )

                # get the columns only
                selected_columns = []
                for reg in selected_regions:
                    if reg not in selected_columns:
                        selected_columns.append(reg.col_name)

                # deleted later
                selected_columns.append("FULL_ADDRESS")
                selected_columns.append("RATIO")

                # remove empty element, if exists
                selected_columns = list(filter(None, selected_columns))
                selected_columns = list(set(selected_columns))

                # if there are possible similar address found
                if addresses.shape[0] > 0:
                    # return the most similar address only
                    if nlargest == 1:

                        # sort the addresses based on the similarity score
                        addresses = addresses.sort_values(
                            by="RATIO", ascending=False
                        ).reset_index(drop=True)

                        return addresses.head(1)[selected_columns].to_dict(
                            orient="list"
                        )

                    # return all the similar addresses
                    else:

                        return (
                            addresses[selected_columns]
                            .nlargest(nlargest, "RATIO")
                            .sort_values(by="RATIO", ascending=False)
                            .to_dict(orient="list")
                        )
                else:
                    return {}

            else:
                raise ValueError(f"The address file can't be found: {parquet_filename}")

        else:
            raise ValueError(
                "No index records found. Make sure the initiation process is succeeded"
            )

    def _load_parquet(self, lat, lon, distance):
        """
        load the rows in the parquet file meeting the condition
        the condition is to ensure the LATITUDE and LONGITUDE are within a distance from the
        argument lat and lon

        Parameters
        ----------
        lat:float
            latitude
        lon:float
            longitude
        distance:integer
                define what the minimum km of distance from the argument lat and lon
        
        Returns
        -------
        a panda dataframe
        """

        local = fs.LocalFileSystem()
        df = pq.read_table(
            self._filenames,
            filesystem=local,
            filters=[
                ("LATITUDE", ">=", lat - distance),
                ("LATITUDE", "<=", lat + distance),
                ("LONGITUDE", ">=", lon - distance),
                ("LONGITUDE", "<=", lon + distance),
            ],
        ).to_pandas()

        return df

    def get_region_by_coordinates(
        self, 
        lat, 
        lon, 
        n=1, 
        km=1, 
        regions=None, 
        operator=None
    ):
        """
        perform coordinate_based matching and return the corresponding regions in a dictionary
        e.g. administrative level or statistical area

        Parameters
        ----------
        lat:float
            latitude
        lon:float
            longitude
        n:integer
            the number of nearest addresses to be returned by the function.
        km:integer
            the nearest addresses will be searched from the input coordinates
            point within the argument kilometer radius
        
        Returns
        -------
        Dictionary    
            a dictionary of addresses with statistical and administrative regions.
            By default, the function will return the record with the smallest
            distance only (n = 1). If no addresses found within the radius (km), 
            the function will return an empty dictionary.
        
        Examples
        --------
        >>> matcher = GeoMatcher(AUS)
        >>> matched = matcher.get_region_by_address(-26.657299,153.094955)
        >>> matched
        {'FULL_ADDRESS': ['8 32 SECOND AVENUE MAROOCHYDORE QLD 4558'],
         'LATITUDE': [-26.6572865955204],
         'LONGITUDE': [153.09496396875],
         'LGA_NAME_2016': ['Sunshine Coast (R)'],
         'SSC_NAME_2016': ['Maroochydore'],
         'SA4_NAME_2016': ['Sunshine Coast'],
         'SA3_NAME_2016': ['Maroochy'],
         'SA2_NAME_2016': ['Maroochydore - Kuluin'],
         'SA1_7DIGITCODE_2016': ['3142707'],
         'MB_CODE_2016': ['30563074700'],
         'DISTANCE': [0.0016422183328786543]}
        """

        min_distance = 0
        # 1 lat equals 110.574km
        distance = (km if km else 1) / 110.574

        # 1. Ensure lat/lon within the country's geo boundary range
        if len(self._hierarchy.coordinate_boundary) != 4:
            raise ValueError("The country's geo boundary is not available")

        within_range = (
            self._hierarchy.coordinate_boundary[0]
            <= lat
            <= self._hierarchy.coordinate_boundary[1]
        ) and (
            self._hierarchy.coordinate_boundary[2]
            <= lon
            <= self._hierarchy.coordinate_boundary[3]
        )
        if not within_range:
            raise ValueError(
                "The latitude input should be within "
                + self._hierarchy.coordinate_boundary[0]
                + "and"
                + self._hierarchy.coordinate_boundary[1]
                + " and longitude input must be within  "
                + self._hierarchy.coordinate_boundary[2]
                + "and"
                + self._hierarchy.coordinate_boundary[3]
            )

        # 2. Make the first load of GNAF dataset
        gnaf_df = self._load_parquet(lat, lon, distance)

        # 2.a If the desired count of addresses not exist, increase the radius
        while gnaf_df.shape[0] < n:
            min_distance = distance
            distance *= 2

            gnaf_df = self._load_parquet(lat, lon, distance)

        # 2.b Keep reducing the size of rows if more than 10k adddresses
        # are found within the radius
        # Take the median distance to reduce
        # This is to limit the number of datapoint to build the Ball tree in the next step
        while gnaf_df.shape[0] >= n + 10000:
            middle_distance = (distance - min_distance) / 2

            temp_df = gnaf_df[
                gnaf_df.LATITUDE.between(lat - middle_distance, lat + middle_distance)
                & gnaf_df.LONGITUDE.between(
                    lon - middle_distance, lon + middle_distance
                )
            ]
            # If no record are found, quit the iteration
            if temp_df.shape[0] < 1:
                break
            gnaf_df = temp_df
            distance = middle_distance

        # 3. Build the Ball Tree and Query for the nearest within k distance
        ball_tree = BallTree(
            np.deg2rad(gnaf_df[["LATITUDE", "LONGITUDE"]].values), metric="haversine"
        )

        distances, indices = ball_tree.query(
            np.deg2rad(np.c_[lat, lon]), k=min(n, gnaf_df.shape[0])
        )
        # Get indices of the search result, Extract pid and calculate distance(km)
        indices = indices[0].tolist()
        pids = gnaf_df.ADDRESS_DETAIL_PID.iloc[indices].tolist()
        distance_map = dict(zip(pids, [distance * 6371 for distance in distances[0]]))

        # 4. Filter the GNAF dataset by address_detail_pid
        bool_list = gnaf_df["ADDRESS_DETAIL_PID"].isin(pids)
        final_gnaf_df = gnaf_df[bool_list]

        final_gnaf_df.loc[:, "DISTANCE"] = final_gnaf_df["ADDRESS_DETAIL_PID"].map(
            distance_map
        )

        return final_gnaf_df.sort_values("DISTANCE").to_dict(orient="list")
