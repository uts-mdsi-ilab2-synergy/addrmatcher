import pandas as pd

states = ["ACT", "NSW", "NT", "OT", "QLD", "SA", "TAS", "VIC", "WA"]

au_ssc = pd.read_csv("SSC_2016_AUST.csv", dtype="str")

# i = 0

for state in states:
    print("--" + state + "--")
    # address detail
    gnaf_address_detail = pd.read_csv(
        state + "_ADDRESS_DETAIL_psv.psv", delimiter="|", dtype="str"
    )
    if state != "OT":
        gnaf_address_detail["STATE"] = state
    else:
        gnaf_address_detail["STATE"] = ""

    # geocode
    gnaf_address_geocode = pd.read_csv(
        state + "_ADDRESS_DEFAULT_GEOCODE_psv.psv", delimiter="|", dtype="str"
    )

    # street locality
    gnaf_street_locality = pd.read_csv(
        state + "_STREET_LOCALITY_psv.psv", delimiter="|", dtype="str"
    )

    # locality
    gnaf_locality = pd.read_csv(state + "_LOCALITY_psv.psv", delimiter="|", dtype="str")

    # mesh block
    gnaf_adress_mb = pd.read_csv(
        state + "_ADDRESS_MESH_BLOCK_2016_psv.psv", delimiter="|", dtype="str"
    )

    gnaf_adress_mb["MB_2016"] = gnaf_adress_mb["MB_2016_PID"].apply(lambda x: x[4:])

    # merge all the datasets into a single dataframe
    gnaf_address_combined = (
        gnaf_address_detail.merge(
            gnaf_street_locality[
                ["STREET_LOCALITY_PID", "STREET_NAME", "STREET_TYPE_CODE"]
            ],
            how="inner",
            on="STREET_LOCALITY_PID",
        )
        .merge(
            gnaf_locality[["LOCALITY_PID", "LOCALITY_NAME"]],
            how="inner",
            on="LOCALITY_PID",
        )
        .merge(
            gnaf_adress_mb[["ADDRESS_DETAIL_PID", "MB_2016"]],
            how="inner",
            on="ADDRESS_DETAIL_PID",
        )
        .merge(
            gnaf_address_geocode[["ADDRESS_DETAIL_PID", "LONGITUDE", "LATITUDE"]],
            how="inner",
            on="ADDRESS_DETAIL_PID",
        )
    )

    gnaf_address_combined["LONGITUDE"] = gnaf_address_combined["LONGITUDE"].astype(
        float
    )
    gnaf_address_combined["LATITUDE"] = gnaf_address_combined["LATITUDE"].astype(float)

    gnaf_address_combined["FULL_ADDRESS"] = (
        gnaf_address_combined[
            [
                "LOT_NUMBER_PREFIX",
                "LOT_NUMBER",
                "LOT_NUMBER_SUFFIX",
                "FLAT_TYPE_CODE",
                "FLAT_NUMBER_PREFIX",
                "FLAT_NUMBER",
                "FLAT_NUMBER_SUFFIX",
                "LEVEL_TYPE_CODE",
                "LEVEL_NUMBER_PREFIX",
                "LEVEL_NUMBER",
                "LEVEL_NUMBER_SUFFIX",
                "NUMBER_FIRST_PREFIX",
                "NUMBER_FIRST",
                "NUMBER_FIRST_SUFFIX",
                "NUMBER_LAST_PREFIX",
                "NUMBER_LAST",
                "NUMBER_LAST_SUFFIX",
                "STREET_NAME",
                "STREET_TYPE_CODE",
                "LOCALITY_NAME",
                "STATE",
                "POSTCODE",
            ]
        ]
        .fillna("")
        .agg("".join, axis=1)
    )

    gnaf_address_combined["CARTESIAN_COOR"] = gnaf_address_combined[
        ["LATITUDE", "LONGITUDE"]
    ].apply(lambda x: cartesian(*list((x.LATITUDE, x.LONGITUDE))), axis=1)
    gnaf_address_combined["CARTESIAN_COOR"] = gnaf_address_combined[
        "CARTESIAN_COOR"
    ].astype(str)

    au_lga = pd.read_csv("LGA_2016_" + state + ".csv", dtype="str")

    mesb_block = pd.read_csv("MB_2016_" + state + ".csv", dtype="str")

    asgs = mesb_block.merge(
        au_lga[["MB_CODE_2016", "LGA_CODE_2016", "LGA_NAME_2016"]],
        how="inner",
        on="MB_CODE_2016",
    ).merge(
        au_ssc[["MB_CODE_2016", "SSC_CODE_2016", "SSC_NAME_2016"]],
        how="inner",
        on="MB_CODE_2016",
    )

    gnaf_address_combined = gnaf_address_combined.merge(
        asgs[
            [
                "LGA_NAME_2016",
                "SSC_NAME_2016",
                "SA4_NAME_2016",
                "SA3_NAME_2016",
                "SA2_NAME_2016",
                "SA1_7DIGITCODE_2016",
                "MB_CODE_2016",
            ]
        ],
        how="inner",
        left_on="MB_2016",
        right_on="MB_CODE_2016",
    )

    # save the combined dataset into a single file

    gnaf_address_combined.to_csv(state + "-GNAF.csv", index=False)
    gnaf_address_combined.to_parquet(state + "-GNAF.parquet", engine="fastparquet")
    # if i == 0:
    #    gnaf_address_combined.to_csv("AU-GNAF.csv", index=False)
    # else:
    #    gnaf_address_combined.to_csv("AU-GNAF.csv", mode='a', header=False, index=False)

    # i = i + 1
# gnaf_address_combined.to_feather('AU-GNAF.feather')
# gnaf_address_combined.to_parquet('AU-GNAF.parquet')
