Introduction
------------
Addrmatcher is an open-source Python software for matching input string addresses to the most similar street addresses and the geo coordinates inputs to the nearest street addresses. The result provides not only the matched addresses, but also the respective country’s different levels of regions for instance - in Australia, government administrative regions, statistical areas and suburb in which the address belongs to. 

The Addrmatcher library is built to work with rapidfuzz, scikit-learn, pandas, numpy and provides user-friendly output. It supports python version 3.6 and above. It runs on all popular operating systems, and quick to install and is free of charge. 

In this initial release, the scope of input data and matching capability are limited to Australian addresses only. The Addrmatcher library will see the opportunity to scale the matching beyond Australia in future. 

The package offers two matching capabilities -
* __address-based matching__ accepts string address as argument.
* __coordinate-based matching__ takes geo coordinate (latitude and longititude) as input.

The development team achieved the optimal speed of matching less than one second for each address and each pair of coordinate input. 

The reference dataset is built upon GNAF(Geocoded National Address File) and ASGS(Australian Statistical Geography Standard) for the Australian addresses. The package users will require to download the optimised format of reference dataset into the working direcory once the package has been installed.

Installation
------------
`pip install addressmatcher`

Data Download
-------------
 Once the package has been installed, the reference dataset needs to be downloaded into __the local current project working directory__ prior to implementation of the package's matching functions. 

 In the command line interface,

`addrmatcher_data_download`

The above console script will download the dataset which is currently hosted in Github into the user's directory. By default, the country is __Australia__ and Australia physical addresses will be downloaded. After executing the command, the 37 parquet files will be stored in directories for example /data/Australia/*.parquet. 
       
Import the package and classes
------------------
```python
# Import the installed package
from addrmatcher import AUS, GeoMatcher, GeoHierarchy

# Initialise the geo region as AUS
matcher = GeoMatcher(AUS)
```

Example - Address-based Matching
--------------------------------
```python
matched_address = matcher.get_region_by_address("9, George Street, North Strathfield, NSW 2137")
print(matched_address)

>{'SA4_NAME_2016': ['Sydney - Inner West'],
 'LGA_NAME_2016': ['Canada Bay (A)'],
 'SA3_NAME_2016': ['Canada Bay'],
 'RATIO': [100.0],
 'STATE': ['NSW'],
 'FULL_ADDRESS': ['9 GEORGE STREET NORTH STRATHFIELD NSW 2137'],
 'SA2_NAME_2016': ['Concord West - North Strathfield'],
 'SSC_NAME_2016': ['North Strathfield'],
 'MB_CODE_2016': ['11205258900'],
 'SA1_7DIGITCODE_2016': ['1138404']}
```

Example - Coordinate-based Matching
-----------------------------------
```python
nearest_address = matcher.get_region_by_coordinates(-29.1789874, 152.628291)
print(nearest_address)

>{'IDX': [129736],
 'FULL_ADDRESS': ['3 7679 CLARENCE WAY MALABUGILMAH NSW 2460'],
 'LATITUDE': [-29.17898685],
 'LONGITUDE': [152.62829132],
 'LGA_NAME_2016': ['Clarence Valley (A)'],
 'SSC_NAME_2016': ['Baryulgil'],
 'SA4_NAME_2016': ['Coffs Harbour - Grafton'],
 'SA3_NAME_2016': ['Clarence Valley'],
 'SA2_NAME_2016': ['Grafton Region'],
 'SA1_7DIGITCODE_2016': ['1108103'],
 'MB_CODE_2016': ['11205732700'],
 'STREET_NAME': ['CLARENCE'],
 'STREET_TYPE_CODE': ['WAY'],
 'LOCALITY_NAME': ['MALABUGILMAH'],
 'STATE': ['NSW'],
 'POSTCODE': ['2460'],
 'ADDRESS_DETAIL_PID': ['GANSW706638188'],
 'FILE_NAME': ['NSW-10.parquet'],
 'DISTANCE': [6.859565028181215e-05]}
```
