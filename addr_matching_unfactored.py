import Levenshtein as lev
import pandas as pd

states = ['ACT','NSW','NT','OT','QLD','SA','TAS','VIC','WA']
gnaf_address_detail = pd.DataFrame()
for state in states:
    gnaf_address_detail_state = pd.read_csv(state+'_ADDRESS_DETAIL_psv.psv',delimiter='|',dtype='str')
    if state != 'OT':
        gnaf_address_detail_state['STATE'] = state
    else:
        gnaf_address_detail_state['STATE'] = ''
        
    if gnaf_address_detail.shape[0] == 0:
        gnaf_address_detail = gnaf_address_detail_state
    else:
        gnaf_address_detail = gnaf_address_detail.append(gnaf_address_detail_state, ignore_index = True)

gnaf_address_geocode = pd.DataFrame()
for state in states:
    gnaf_address_geocode_state = pd.read_csv(state+'_ADDRESS_DEFAULT_GEOCODE_psv.psv',delimiter='|',dtype='str')
    if gnaf_address_geocode.shape[0] == 0:
        gnaf_address_geocode = gnaf_address_geocode_state
    else:
        gnaf_address_geocode = gnaf_address_geocode.append(gnaf_address_geocode_state, ignore_index = True)

gnaf_street_locality = pd.DataFrame()
for state in states:
    gnaf_street_locality_state = pd.read_csv(state+'_STREET_LOCALITY_psv.psv',delimiter='|',dtype='str')
    if gnaf_street_locality.shape[0] == 0:
        gnaf_street_locality = gnaf_street_locality_state
    else:
        gnaf_street_locality = gnaf_street_locality.append(gnaf_street_locality_state, ignore_index = True)
        
gnaf_locality = pd.DataFrame()
for state in states:
    gnaf_locality_state = pd.read_csv(state+'_LOCALITY_psv.psv',delimiter='|',dtype='str')
    if gnaf_locality.shape[0] == 0:
        gnaf_locality = gnaf_locality_state
    else:
        gnaf_locality = gnaf_locality.append(gnaf_locality_state, ignore_index = True)
        
gnaf_adress_mb = pd.DataFrame()
for state in states:
    gnaf_adress_mb_state = pd.read_csv(state+'_ADDRESS_MESH_BLOCK_2016_psv.psv',delimiter='|',dtype='str')
    if gnaf_adress_mb.shape[0] == 0:
        gnaf_adress_mb = gnaf_adress_mb_state
    else:
        gnaf_adress_mb = gnaf_adress_mb.append(gnaf_adress_mb_state, ignore_index = True)


#merge all the datasets into a single dataframe
gnaf_address_combined = gnaf_address_detail\
    .merge(gnaf_street_locality[['STREET_LOCALITY_PID','STREET_NAME','STREET_TYPE_CODE']], 
           how = 'inner', on = 'STREET_LOCALITY_PID')\
    .merge(gnaf_locality[['LOCALITY_PID','LOCALITY_NAME']], 
           how = 'inner', on = 'LOCALITY_PID')\
    .merge(gnaf_adress_mb[['ADDRESS_DETAIL_PID','MB_2016_PID']], 
           how = 'inner', on = 'ADDRESS_DETAIL_PID')
           
#replace NaN/None with emptry string, for string concatenation purposes
gnaf_address_combined.fillna('', inplace=True)

gnaf_address_combined['FULL_ADDRESS'] = gnaf_address_combined['LOT_NUMBER_PREFIX'].astype(str) + gnaf_address_combined['LOT_NUMBER'].astype(str)+\
                                    gnaf_address_combined['LOT_NUMBER_SUFFIX'].astype(str) + gnaf_address_combined['FLAT_TYPE_CODE'].astype(str) +\
                                    gnaf_address_combined['FLAT_NUMBER_PREFIX'].astype(str) + gnaf_address_combined['FLAT_NUMBER'].astype(str) +\
                                    gnaf_address_combined['FLAT_NUMBER_SUFFIX'].astype(str) + gnaf_address_combined['LEVEL_TYPE_CODE'].astype(str) +\
                                    gnaf_address_combined['LEVEL_NUMBER_PREFIX'].astype(str) + gnaf_address_combined['LEVEL_NUMBER'].astype(str) + \
                                    gnaf_address_combined['LEVEL_NUMBER_SUFFIX'].astype(str) + gnaf_address_combined['NUMBER_FIRST_PREFIX'].astype(str) +\
                                    gnaf_address_combined['NUMBER_FIRST'].astype(str) + gnaf_address_combined['NUMBER_FIRST_SUFFIX'].astype(str) +\
                                    gnaf_address_combined['NUMBER_LAST_PREFIX'].astype(str) + gnaf_address_combined['NUMBER_LAST'].astype(str) + \
                                    gnaf_address_combined['NUMBER_LAST_SUFFIX'].astype(str) + ' '+ gnaf_address_combined['STREET_NAME'].astype(str) +' '+\
                                    gnaf_address_combined['STREET_TYPE_CODE'].astype(str) + ', '+ gnaf_address_combined['LOCALITY_NAME'].astype(str) +', '+\
                                    gnaf_address_combined['STATE'].astype(str)+' '+gnaf_address_combined['POSTCODE'].astype(str)


#statistical area (ASGS 2016: https://www.abs.gov.au/AUSSTATS/abs@.nsf/DetailsPage/1270.0.55.001July%202016?OpenDocument)
mesb_block = pd.DataFrame()
for state in states:
    mesb_block_state = pd.read_csv('MB_2016_'+state+'.csv',dtype='str')
    if mesb_block.shape[0] == 0:
        mesb_block = mesb_block_state
    else:
        mesb_block = mesb_block.append(mesb_block_state, ignore_index = True)
        
au_ssc = pd.read_csv('SSC_2016_AUST.csv', dtype='str')

asgs = mesb_block.merge(au_lga[['MB_CODE_2016','LGA_CODE_2016','LGA_NAME_2016']], how = 'inner', on='MB_CODE_2016')\
                 .merge(au_ssc[['MB_CODE_2016','SSC_CODE_2016','SSC_NAME_2016']], how = 'inner', on='MB_CODE_2016')

#save the combined dataset into a single file
gnaf_address_combined.to_csv('AU-GNAF.csv')
#gnaf_address_combined.to_feather('AU-GNAF.feather')
#gnaf_address_combined.to_parquet('AU-GNAF.parquet')


AU = {}
AU['MESH_BLOCK'] = 'MB_CODE_2016'
AU['SA1'] = 'SA1_7DIGITCODE_2016'
AU['SA2'] = 'SA2_NAME_2016'
AU['SA3'] = 'SA3_NAME_2016'
AU['SA4'] = 'SA4_NAME_2016'
AU['SUBURB'] = 'SSC_NAME_2016'
AU['LGA'] = 'LGA_NAME_2016'
AU['STATE'] = 'STATE_NAME_2016'


country = AU

def getRegionByAddress(address, sim_threshold = 0.95, top_result = True):
    """
    match the given address with the available/reference datasets to 
    obtain the corresponding region. 
    :return: list of region (dictionary)
    """
    
    #match the address 
    gnaf_address_combined['SIM_RATIO'] = gnaf_address_combined['FULL_ADDRESS'].\
        apply(lambda x : lev.ratio(address.lower(),x.lower()))

    if gnaf_address_combined.shape[0] > 0:
        if top_result:
            res = gnaf_address_combined[gnaf_address_combined['SIM_RATIO'] > sim_threshold]
            res = res.sort_values(by='SIM_RATIO', ascending=False)
            mb = res.iloc[0,39] #get meshblock

            region = asgs[asgs['MB_CODE_2016'] == mb[4:]]
            
            for key, value in AU.items():
                print(key+":"+region.iloc[0][value])
        
        else:
            #show all records

    else:
        return 'No Address Matched'
        
#Ona Coffee
getRegionByAddress('3 Ultimo Rd, Haymarket, NSW 2000', sim_threshold = 0.95)
#Galleon Cafe
getRegionByAddress('9 Carlisl St, St Kild VIC 3182', sim_threshold = 0.8)