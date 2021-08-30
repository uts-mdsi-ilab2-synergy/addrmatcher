from matcher import GeoMatcher
from operator import lt, le, ge, gt
from AU import AUS
import time


# filename = ["ACT-GNAF.csv","NSW-GNAF.csv","NT-GNAF.csv","OT-GNAF.csv","QLD-GNAF.csv",
#          "SA-GNAF.csv","TAS-GNAF.csv","VIC-GNAF.csv","WA-GNAF.csv"]

filename = ["/data/ACT-GNAF.csv"]
matcher = GeoMatcher(AUS, filename)
