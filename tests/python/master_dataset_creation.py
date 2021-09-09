import pandas as pd
import math

#maximum number of records in a parquet file (except the index file)
max_rows = 500000
#states = ["ACT", "NSW", "NT", "OT", "QLD", "SA", "TAS", "VIC", "WA"]
states = ["ACT", "WA"]

#initiate the index file
index_file = pd.DataFrame(columns=['IDX','STREET_NAME','STREET_TYPE_CODE','LOCALITY_NAME','STATE','POSTCODE','FILE_NAME','ADDRESS_COUNT'])
index = 0

#suburb
au_ssc = pd.read_csv("SSC_2016_AUST.csv", dtype="str")

def cartesian(latitude, longitude, elevation=0):
    # Convert to radians
    latitude = latitude * (math.pi / 180)
    longitude = longitude * (math.pi / 180)

    R = 6371  # 6378137.0 + elevation  # relative to centre of the earth
    X = R * math.cos(latitude) * math.cos(longitude)
    Y = R * math.cos(latitude) * math.sin(longitude)
    Z = R * math.sin(latitude)
    return (X, Y, Z)



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
        .agg(" ".join, axis=1)
    )
    
    gnaf_address_combined["FULL_ADDRESS"] = gnaf_address_combined["FULL_ADDRESS"].replace('\s+', ' ', regex=True).str.strip()

    gnaf_address_combined["CARTESIAN_COOR"] = gnaf_address_combined[
        ["LATITUDE", "LONGITUDE"]
    ].apply(lambda x: cartesian(*list((x.LATITUDE, x.LONGITUDE))), axis=1)
    gnaf_address_combined["CARTESIAN_COOR"] = gnaf_address_combined[
        "CARTESIAN_COOR"
    ].astype(str)

    #LGA
    au_lga = pd.read_csv("LGA_2016_" + state + ".csv", dtype="str")

    #mesh block
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
    
    #group the address to create a unique combination of
    #street name, locality/suburb, state, and postcode
    gnaf_address_index = gnaf_address_combined.groupby(['STREET_NAME','STREET_TYPE_CODE','LOCALITY_NAME','STATE','POSTCODE'])\
                            .size()\
                            .to_frame(name = 'ADDRESS_COUNT')\
                            .sort_values(by='ADDRESS_COUNT', ascending = False)\
                            .reset_index()
    
    #if a state has less than max rows, then create a single file to store the state's addresses
    if gnaf_address_combined.shape[0] <= max_rows:
        #set the parquet filename
        filename = state+"-1.parquet"
        
        state_idx_file = gnaf_address_index[['STREET_NAME','STREET_TYPE_CODE','LOCALITY_NAME','STATE','POSTCODE','ADDRESS_COUNT']]
        state_idx_file.insert(0, 'IDX', range(index + 1, index + 1 + len(state_idx_file)))
        state_idx_file.insert(6, 'FILE_NAME', filename)
        
        index_file = index_file.append(state_idx_file, ignore_index = True)
        
        index = index + len(state_idx_file)
        
        #loop to generate the index
        #for i, row in gnaf_address_index.iterrows():
        #    index = index + 1
        #    index_row = {'IDX':index,
        #                 'STREET_NAME':row['STREET_NAME'],
        #                 'STREET_TYPE_CODE':row['STREET_TYPE_CODE'],
        #                 'LOCALITY_NAME':row['LOCALITY_NAME'],
        #                 'STATE':row['STATE'],
        #                 'POSTCODE':row['POSTCODE'],
        #                 'FILE_NAME':filename,
        #                 'ADDRESS_COUNT':row['NB_RECORDS']}
                         
        #    index_file = index_file.append(index_row, ignore_index = True)
        
        #combine with the index to get the IDX 
        gnaf_address_combined = (gnaf_address_combined
                                    .merge(state_idx_file, 
                                            how='inner', 
                                            on=['STREET_NAME',
                                                'STREET_TYPE_CODE',
                                                'LOCALITY_NAME',
                                                'STATE',
                                                'POSTCODE']))
                                                
        gnaf_address_combined['FILE_NAME'] = filename
        
        #save into parquet
        gnaf_address_combined[[ 'IDX',
                                'FULL_ADDRESS',
                                'LATITUDE',
                                'LONGITUDE',
                                'LGA_NAME_2016',
                                'SSC_NAME_2016',
                                'SA4_NAME_2016',
                                'SA3_NAME_2016',
                                'SA2_NAME_2016',
                                'SA1_7DIGITCODE_2016',
                                'MB_CODE_2016',
                                'STREET_NAME',
                                'STREET_TYPE_CODE',
                                'LOCALITY_NAME',
                                'STATE',
                                'POSTCODE',
                                'CARTESIAN_COOR',
                                'FILE_NAME']].to_parquet(filename, engine="fastparquet")
    else:
        
        #initiate
        nb_rows_per_file = 0
        nb_files = 1
        filename = state+"-"+str(nb_files)+".parquet"
        
        state_idx_file = gnaf_address_index[['STREET_NAME','STREET_TYPE_CODE','LOCALITY_NAME','STATE','POSTCODE','ADDRESS_COUNT']]
        state_idx_file.insert(0, 'IDX', range(index + 1, index + 1 + len(state_idx_file)))
        state_idx_file.insert(6, 'FILE_NAME', "")
        
        index = index + len(state_idx_file)
        
        #split_file = pd.DataFrame(columns=['IDX','STREET_NAME','STREET_TYPE_CODE','LOCALITY_NAME','STATE','POSTCODE','FILE_NAME','ADDRESS_COUNT'])
        
        chunk_id = []
        
        for i, row in state_idx_file.iterrows():
            if row['ADDRESS_COUNT'] + nb_rows_per_file > max_rows:
                #save the current chunk into a parquet file
                gnaf_address_split = gnaf_address_combined\
                                    .merge(state_idx_file[state_idx_file['IDX'].isin(chunk_id)], 
                                            how='inner', 
                                            on=['STREET_NAME',
                                                'STREET_TYPE_CODE',
                                                'LOCALITY_NAME',
                                                'STATE',
                                                'POSTCODE'])
                                                
                gnaf_address_split['FILE_NAME'] = filename                                
                gnaf_address_split[['IDX',
                                    'FULL_ADDRESS',
                                    'LATITUDE',
                                    'LONGITUDE',
                                    'LGA_NAME_2016',
                                    'SSC_NAME_2016',
                                    'SA4_NAME_2016',
                                    'SA3_NAME_2016',
                                    'SA2_NAME_2016',
                                    'SA1_7DIGITCODE_2016',
                                    'MB_CODE_2016',
                                    'STREET_NAME',
                                    'STREET_TYPE_CODE',
                                    'LOCALITY_NAME',
                                    'STATE',
                                    'POSTCODE',
                                    'CARTESIAN_COOR',
                                    'FILE_NAME']].to_parquet(filename, engine="fastparquet")
                
                #update filename
                state_idx_file.loc[state_idx_file['IDX'].isin(chunk_id),'FILE_NAME'] = filename
                                    
                #initiate a new chunk
                nb_files = nb_files+1
                filename = state+"-"+str(nb_files)+".parquet"
                
                nb_rows_per_file = 0
                chunk_id = []
            
            #add address to a new chunk
            chunk_id.append(row["IDX"])
            nb_rows_per_file = nb_rows_per_file + row['ADDRESS_COUNT']
                    
        gnaf_address_split = gnaf_address_combined\
                                .merge(state_idx_file[state_idx_file['IDX'].isin(chunk_id)], 
                                    how='inner', 
                                    on=['STREET_NAME',
                                        'STREET_TYPE_CODE',
                                        'LOCALITY_NAME',
                                        'STATE',
                                        'POSTCODE'])
        
        gnaf_address_split['FILE_NAME'] = filename          
        gnaf_address_split[['IDX',
                            'FULL_ADDRESS',
                            'LATITUDE',
                            'LONGITUDE',
                            'LGA_NAME_2016',
                            'SSC_NAME_2016',
                            'SA4_NAME_2016',
                            'SA3_NAME_2016',
                            'SA2_NAME_2016',
                            'SA1_7DIGITCODE_2016',
                            'MB_CODE_2016',
                            'STREET_NAME',
                            'STREET_TYPE_CODE',
                            'LOCALITY_NAME',
                            'STATE',
                            'POSTCODE',
                            'CARTESIAN_COOR',
                            'FILE_NAME']].to_parquet(filename, engine="fastparquet") 
        
        #update filename
        state_idx_file.loc[state_idx_file['IDX'].isin(chunk_id),'FILE_NAME'] = filename
        
        index_file = index_file.append(state_idx_file, ignore_index = True)


#save the index into a parquet file
index_file["ADDRESS"] = index_file[["STREET_NAME",
                             "STREET_TYPE_CODE",
                             "LOCALITY_NAME",
                             "STATE",
                             "POSTCODE"]]\
                        .fillna("")\
                        .agg(" ".join, axis=1)
    

index_file.to_parquet("index.parquet", engine="fastparquet")   