from region import Region
        
class GeoHierarchy:

    __slot__ = ("_type", "_root")
    
    def __init__(self):
        self._type = {}
        self._root = {}
    
    @property
    def type(self):
        return self._type
    
    def add_type(self, type_id, type_name = ""):
        if type_id in self._type:
            raise ValueError("Duplicate key ("+type_id+") found")
        
        if type_name != "":
            self._type[type_id] = type_name
        else:
            self._type[type_id] = type_id
        
        self._root[type_id] = None
        
    def add_region(self, type_id, region, parent_region = None):
        if type_id not in self._type:
            raise KeyError("Value key ("++") not found")
        
        
        #set root
        if parent_region is None:
            if self._root[type_id] is not None:
                raise ValueError("Duplicate root found")
            else:
                #root
                self._root[type_id] = region
        
        #add child to parent
        if parent_region is not None:
            if self._root[type_id] is None:
                raise ValueError("Parent is undefined. Root isn't exists yet")
            else:
                parent_region.add_sub_region(region)
    
    def get_all_regions(self, type_id = ""):
        if type_id == "":
            for id in self._type:
                return self._get_nodes(self._root[id])
        else:
            return self._get_nodes(self._root[type_id])
                
    def _get_nodes(self, hierarchy):
        print(hierarchy)
        for child in hierarchy.sub_region:
            return self._get_nodes(child)
    
        
        