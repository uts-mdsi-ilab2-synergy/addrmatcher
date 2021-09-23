from region import Region
from operator import lt, le, ge, gt
        
class GeoHierarchy:

    __slot__ = ("_type","_type_root","_country", "_country_name", "_coordinate_boundary")
    
    def __init__(self, country, name, coordinate_boundary = None):
        self._type = {}
        self._type_root = {}
        self._country = self._Node(country)
        self._country_name = name
        self.coordinate_boundary = coordinate_boundary
        
    
    @property
    def type(self):
        return self._type
        
    @property
    def name(self):
        return self._country_name
        
    @property
    def coordinate_boundary(self):
        return self._coordinate_boundary
        
    @coordinate_boundary.setter
    def coordinate_boundary(self, value):
        if value is not None:
            if isinstance(value,list) and len(value):
                if (value[0] < value[1]) and (value[2] < value[3]):
                    self._coordinate_boundary = value
                else:
                    if value[0] >= value[1]:
                        raise ValueError("The latitute range is invalid (lat_max >= lat_min)")
                    else:
                        raise ValueError("The longitude range is invalid (long_max >= long_min)")
            else:
                raise ValueError("The boundary must be a four element list [lat_min, lat_max, long_min, long_max]")
    
    def add_type(self, region, type_id, type_name = ""):
        """
        add a new geographical hierarchy type, for instace: statistical area, administrative level
        :param Region region: The root region for the hierarchy.
                              The common upper level region name which is shared with other 
                              hierarchy can't be assigned as a root. 
                              For instance, administrative level and statistical area use 
                              Country or State/Province as their upper regional level
        :param string type_id: a unique identifier for the type
        :param string type_name: 
        
        """
        
        
        #validate the arguments
        if type_id.strip() == "":
            raise ValueError("Type id cannot be empty")
            
        if not isinstance(region, Region):
            raise ValueError("`region` must be a Region")
        
        if type_id in self._type:
            raise ValueError("Duplicate type ("+type_id+") found")
        
        if (type_name.strip() != ""):
            self._type[type_id] = type_name
        else:
            self._type[type_id] = type_id
            
        self._type_root[type_id] = region
        
    def add_region(self, region, parent_region):
        """
        add a region as a child/sub region of another region
        
        :param Region region: the sub region to be added as a child of the parent region
        :param Region parent_region: 
        """
        
        #validate the arguments
        if not isinstance(region, Region):
            raise ValueError("`region` must be a Region")
            
        if not isinstance(parent_region, Region):
            raise ValueError("`parent_region` must be a Region")
        
        #if the parent region is the root of the hierarchy (country)
        #then add the new region as a sub region of the root
        if parent_region == self._country.region:
            self._country.add_child(self._Node(region))
        else:
            #find the node related to the parent_region
            parent_node = self._country.get_node_by_region(parent_region)
            if parent_node is not None:
                parent_node.add_child(self._Node(region))
            else:
                raise ValueError(str(parent_region)+" is not found")
        
    def get_regions_by_name(self, operator = None, name = "", names = [], attribute = ""):
        """
        get all the relevant regions from the hierarchy based on the given parameters
        
        :param Operator operator: use the operator to find all the upper/lower level regions 
                                  from a particular region name.
                                  For instance: Country > State (Country gt State).
                                                Use the 'gt' operator to search for the upper level of State
        :param string name: fill the name or short name of the regions in relations to operator parameter above
        :param list names: the list of region's name to look for
        :param string attribute: the region's attribute name that will be saved into the list (name, short_name, or col_name)
                                 if it's empty, then the list will store the object of the region
        :return list: list of regions or region's attribute                   
        """
        
        #validate the arguments
        if operator is not None and len(names) > 0:
            raise ValueError("Operator has to be empty if list of `names` is provided. vice versa.")
            
        if operator is not None and name.strip() == "":
            raise ValueError("`name` must be provided if operator is not empty. vice versa.")
        
        if operator is not None:
            if operator not in [lt, le, ge, gt]:
                raise ValueError("Invalid operator value. Select one of lt, le, ge, or lt")
        
        if attribute.strip() != "":
            if attribute not in ["name","short_name","col_name"]:
                raise ValueError("Invalid attribute value. Select one of name, short_name, or col_name")
        
        regions = []        
        if len(names) > 0:
            for reg_name in names:
                node = self._country.get_node_by_name(reg_name)
                if node is not None:
                    if (attribute.strip() == ""):
                        regions.append(node.region)
                    else:
                        regions.append(getattr(node.region,attribute))
                else:
                    ValueError("Region is not found")
            
        elif name != "":
            reference_node = self._country.get_node_by_name(name)
            if reference_node is not None:
                if operator in [gt,ge]:
                    nodes = self._country.get_all_nodes(lowest_region=reference_node.region)
                    if (attribute.strip() == ""):
                        for node in nodes:
                            regions.append(node.region)
                    else:
                        for node in nodes:
                            regions.append(getattr(node.region,attribute))
                else:
                    nodes = reference_node.get_all_nodes()
                    if (attribute.strip() == ""):
                        for node in nodes:
                            regions.append(node.region)
                    else:
                        for node in nodes:
                            regions.append(getattr(node.region,attribute))
            else:
                    ValueError("Region is not found")

        else:
            nodes = self._country.get_all_nodes()
            if (attribute.strip() == ""):
                for node in nodes:
                    regions.append(node.region)
            else:
                for node in nodes:
                    regions.append(getattr(node.region,attribute))
                    
        return regions
        
    def get_smallest_region_boundaries(self):
        return self._country.get_leaf_node().region
        
        
    class _Node:
        
        __slot__ = ("_region","_child")
        
        def __init__(self, region, child = []):
            self._region = region
            
            if len(child) > 0:
                self._child = child
            else:
                self._child = []
        
        @property
        def region(self):
            return self._region
            
        @property
        def child(self):
            return self._child
        
        def add_child(self, child):
            self._child.append(child)
            
        def get_node_by_region(self, region):
            #print("-->"+str(self._region))
            if self._region == region:
                return self
            else:
                for child in self._child:
                    #print("-->child:"+str(child._region))
                    match = child.get_node_by_region(region)
                    if match is not None:
                        return match
            
            #print("Node "+str(region)+" is not found")
            return None
            
            
        def get_node_by_name(self, name):
            if self._region.name.lower() == name.lower() or self._region.short_name.lower() == name.lower():
                return self
            else:
                for child in self._child:
                    match = child.get_node_by_name(name)
                    if match is not None:
                        return match
                        
            return None
            
        def get_all_nodes(self, lowest_region = None):
        
            nodes = []
            if len(self._child) == 0:
                return [self]
            elif (self._region == lowest_region):
                    return [self]
            else:
                nodes += [self]
                for child in self._child:
                    if child not in nodes:
                        nodes += child.get_all_nodes(lowest_region)
                    
            return nodes
            
        def get_leaf_node(self):
            if len(self._child) == 0:
                return self
            else:
                for child in self._child:
                    match = child.get_leaf_node()
                    if match is not None:
                        return match
            
        def __str__(self):
            return str(self._region+". child:"+str(len(self._child)))
        
        def __eq__(self, other):
            return self._region == other._region