import Levenshtein as lev
import pandas as pd

gnaf_address_combined = pd.read_csv("AU-GNAF.csv", dtype='str')

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