Addrmatcher is an open-source Python software for matching input string addresses to the most similar street addresses and the geo coordinates inputs to the nearest street addresses. The result provides not only the matched addresses, but also the respective country’s different levels of regions for instance - in Australia, government administrative regions, statistical areas and suburb in which the address belongs to. 

The Addrmatcher library is built to work with rapidfuzz, scikit-learn, pandas, numpy and provides user-friendly output. It supports python version 3.6 and above. It runs on all popular operating systems, and quick to install and is free of charge. 

In this initial release, the scope of input data and matching capability are limited to Australian addresses only. The Addrmatcher library will see the opportunity to scale the matching beyond Australia in future. 

The package offers two functions - one performs address-based matching accepting string addresses argument, and the other does coordinate_based matching by taking geo coordinates (latitude and longititude) input.

The development team achieved the optimal speed of matching less than one second for each address and each pair of coordinate input. 

The reference dataset is built upon GNAF(Geocoded National Address File) and ASGS(Australian Statistical Geography Standard) for the Australian addresses. The package users will require to download the optimised format of reference dataset into the working direcory once the package has been installed.

Installation
------------

`pip install addressmatcher`

Data Download
-------------
 Once the package has been install, the reference dataset needs to be downloaded prior to implementation of the package's matching functions. 
 In the command line interface, in the project working directory, 

`addressmatcher_data_download`
By default, the country is __Australia__ and Australia physical addresses will be downloaded. After execution the command, the 37 parquet files will be stored in directories for example /data/AU/*.parquet
