from region import Region
from hierarchy import GeoHierarchy
from matcher import GeoMatcher
from operator import lt, le, ge, gt
from AU import AUS
import time

from addrmatcher_package import matcher, addrmatcher

matcher.GeoMatcher.get_region_by_address(
    "UNIT 410 GEORGINA CRESCENT KALEEN ACT 2617", top_result=True
)
# addrmatcher.GeoMatcher.get_region_by_address(
#     "UNIT 410 GEORGINA CRESCENT KALEEN ACT 2617", top_result=True
# )


# print("--init--")
# start_time = time.time()
# # filename = ["ACT-GNAF.csv","NSW-GNAF.csv","NT-GNAF.csv","OT-GNAF.csv","QLD-GNAF.csv",
# #           "SA-GNAF.csv","TAS-GNAF.csv","VIC-GNAF.csv","WA-GNAF.csv"]
# filename = ["ACT-GNAF.csv", "WA-GNAF.csv"]
# matcher = GeoMatcher(AUS, filename)
# print("--- %s seconds ---" % (time.time() - start_time))


# # print("--match--")
# # start_time = time.time()
# # # matcher.get_region_by_address('UNIT 410 GEORGINA CRESCENT KALEEN ACT 2617',regions=["SA4","SA3","SA2","Suburb"])
# # # matcher.get_region_by_address('UNIT 410 GEORGINA CRESCENT KALEEN ACT 2617',operator=le,region="LGA")

# # matcher.get_region_by_address(
# #     "UNIT 410 GEORGINA CRESCENT KALEEN ACT 2617", top_result=False
# # )
# # print("--- %s seconds ---" % (time.time() - start_time))


# # print("--match by lat/lon--")
# # start_time = time.time()
# # matcher.get_region_by_coordinates(-29.178987, 152.628291)
# # print("--- %s seconds ---" % (time.time() - start_time))
