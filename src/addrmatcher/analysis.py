from .matcher import GeoMatcher
from pyarrow import fs
import pyarrow.parquet as pq
import numpy as np
from urllib import parse
from collections import Counter
from PIL import Image
import requests
import io


class Analysis(GeoMatcher):
    def get_map_data_by_address(
        self,
        address,
        apikey,
        similarity_threshold=0.9,
        top_result=True,
        regions=[],
        operator=None,
        region="",
        address_cleaning=False,
        string_metric="levenshtein",
    ):

        geocode_dict = self._get_geocode_by_address(
            address,
            similarity_threshold=similarity_threshold,
            top_result=top_result,
            regions=regions,
            operator=operator,
            region=region,
            address_cleaning=address_cleaning,
            string_metric=string_metric,
        )

        img = self._get_static_map_by_coord(
            geocode_dict["LATITUDE"], geocode_dict["LONGITUDE"], apikey
        )
        if not img:
            return None
        return self._map_analysis(img)

    def _get_static_map_by_coord(
        self, lat, lon, apikey, zoom=13, size=(400, 400), maptype="roadmap"
    ):
        style_dict = {
            "landscape.man_made": "#CC9999",
            "landscape.natural": "#66FF00",
            "poi.attraction": "#99CCFF",
            "poi.business": "#99FF99",
            "poi.medical": "#FF0000",
            "poi.park": "#009900",
            "poi.school": "#CC0033",
            "poi.sports_complex": "#993300",
            "road.arterial": "#669999",
            "road.highway": "#3399CC",
            "road.local": "#FFCCCC",
            "transit.line": "#66FFCC",
            "transit.station.airport": "#FFCC99",
            "transit.station.bus": "#FFFF99",
            "transit.station.rail": "#800080",
            "water": "#00FFFF",
        }

        url = "https://maps.googleapis.com/maps/api/staticmap"
        assert maptype in (
            "roadmap",
            "satellite",
            "hybrid",
            "terrain",
        ), "maptype must be one of 'roadmap', 'statelite', 'hybrid' and 'terrain'"

        params = {
            "center": f"{lat},{lon}",
            "zoom": zoom,
            "maptype": maptype,
            "size": f"{size[0]}x{size[1]}",
            "key": apikey,
        }

        url += f"?{parse.urlencode(params)}"
        for style, colour in style_dict.items():
            url += f"&style=feature:{style}|color:0x{colour.lstrip('#').lower()}"
        r = requests.get(url, params=params)
        if r.status_code != 200:
            print(f"Status Code {r.status_code}")
            return
        img = Image.open(io.BytesIO(r.content)).convert("RGB")
        return img

    def _get_geocode_by_address(
        self,
        address,
        similarity_threshold=0.9,
        top_result=True,
        regions=[],
        operator=None,
        region="",
        address_cleaning=False,
        string_metric="levenshtein",
    ):

        address_df = self.get_region_by_address(
            address,
            similarity_threshold=similarity_threshold,
            top_result=top_result,
            regions=regions,
            operator=operator,
            region=region,
            address_cleaning=address_cleaning,
            string_metric=string_metric,
        )

        if not address_df:
            return
        addr_list = address_df.get("FULL_ADDRESS")
        if not addr_list:
            return
        addr = addr_list[0]
        local = fs.LocalFileSystem()
        geocode_dict = {
            k: v[0]
            for k, v in pq.read_table(
                self._filename,
                columns=["FULL_ADDRESS", "LATITUDE", "LONGITUDE"],
                filesystem=local,
                filters=[("FULL_ADDRESS", "==", addr)],
            )
            .to_pydict()
            .items()
        }
        return geocode_dict

    def _map_analysis(self, img):
        col_dict = {
            (203, 152, 152): "landscape.man_made",
            (102, 254, 0): "landscape.natural",
            (153, 204, 255): "poi.attraction",
            (153, 255, 153): "poi.business",
            (255, 0, 0): "poi.medical",
            (0, 153, 0): "poi.park",
            (204, 0, 51): "poi.school",
            (153, 51, 0): "poi.sports_complex",
            (102, 152, 152): "road.arterial",
            (51, 153, 203): "road.highway",
            (249, 198, 198): "road.local",
            (102, 253, 202): "transit.line",
            (254, 203, 153): "transit.station.airport",
            (255, 255, 153): "transit.station.bus",
            (128, 0, 128): "transit.station.rail",
            (0, 254, 254): "water",
        }
        img_array = np.asarray(img).reshape(-1, 3)
        total = img_array.shape[0]
        colors = dict(Counter(map(tuple, img_array)))
        area_dict = {}
        for ary, count in list(colors.items()):
            for k, v in col_dict.items():
                if ary in colors:
                    if np.sqrt(np.mean((np.array(ary) - np.array(k)) ** 2)) < 7:
                        area_dict[v] = area_dict[v] + count if v in area_dict else count
                        colors.pop(ary)
        area_dict = {k: v / total for k, v in area_dict.items()}
        if not area_dict:
            return
        return area_dict
