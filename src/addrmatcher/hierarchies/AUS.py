from ..region import Region
from ..hierarchy import GeoHierarchy

country = Region("Country", "National")

# administrative level
state = Region("State", "State", "STATE")
lga = Region("Local Government Area", "LGA", "LGA_NAME_2016")
suburb = Region("Suburb", "Suburb", "SSC_NAME_2016")

# statistical area
sa4 = Region("Statistical Area 4", "SA4", "SA4_NAME_2016")
sa3 = Region("Statistical Area 3", "SA3", "SA3_NAME_2016")
sa2 = Region("Statistical Area 2", "SA2", "SA2_NAME_2016")
sa1 = Region("Statistical Area 1", "SA1", "SA1_7DIGITCODE_2016")

mb = Region("Meshblock", "MB", "MB_CODE_2016")

AUS = GeoHierarchy(country, "Australia")

AUS.add_region(region=state, parent_region=country)

# administrative
AUS.add_region(region=lga, parent_region=state)
AUS.add_region(region=suburb, parent_region=lga)
AUS.add_region(region=mb, parent_region=suburb)

# statistical area
AUS.add_region(region=sa4, parent_region=state)
AUS.add_region(region=sa3, parent_region=sa4)
AUS.add_region(region=sa2, parent_region=sa3)
AUS.add_region(region=sa1, parent_region=sa2)
AUS.add_region(region=mb, parent_region=sa1)

AUS.add_type(lga, "Administrative")
AUS.add_type(sa4, "ASGS", "Australian Statistical Geography Standard")

AUS.set_coordinate_boundary(-43.58301104, -9.23000371, 96.82159219, 167.99384663)
