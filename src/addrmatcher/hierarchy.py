from .region import Region
from operator import le, ge


class GeoHierarchy:
    """
    The GeoHierarchy class represents the structure of a country's region/area;
    for instance, a state or a province is the sub-region of a country.
    """

    __slots__ = (
        "_types",
        "_type_root",
        "_country",
        "_country_name",
        "_coordinate_boundary",
    )

    def __init__(self, country, name, coordinate_boundary=None):
        # country is the root of the hierarchy
        self._types = {}
        self._type_root = {}
        self._country = self._Node(country)
        self._country_name = name
        if coordinate_boundary is not None:
            self.set_coordinate_boundary(*coordinate_boundary)

    @property
    def types(self):
        """
        Return the dictionary that contains all the defined regional structures.
        
        Returns
        -------
        dictionary
            The defined regional structures
        
        Examples
        --------
        >>> country = Region("Country")
        >>> state = Region("State",col_name="STATE")
        >>> sa4 = Region("Statistica Area 4",short_name="SA4",col_name="SA4")
        >>> australia = GeoHierarchy(country,"Australia")
        >>> australia.add_region(region=state, parent_region=country)
        >>> australia.add_region(region=sa4, parent_region=state)
        >>> australia.add_type(sa4,"ASGS","Australian Statistical Geography Standard")
        >>> australia.type
        {'ASGS': 'Australian Statistical Geography Standard'}
        """
        return self._types

    @property
    def name(self):
        """
        Return the name of the country
        
        Returns
        -------
        string
            The name of the country
        
        Examples
        --------
        The country's name can be set initially when calling the constructor.
        >>> australia = GeoHierarchy(country,"Australia")
        >>> australia.name
        'Australia'
        """
        return self._country_name

    @property
    def coordinate_boundary(self):
        """
        Return the list of coordinates as the boundary of a country.
        The format is [minimum latitude, maximum latitude, minimum longitude,
        maximum longitude]
        
        Returns
        -------
        string
            The coordinate boundary of a country
        
        Examples
        --------
        >>> australia = GeoHierarchy(country,"Australia")
        >>> australia.set_coordinate_boundary(-43.58301104, -9.23000371,
                                              96.82159219, 167.99384663)
        >>> australia.coordinate_boundary
        [-43.58301104, -9.23000371, 96.82159219, 167.99384663]
        """
        return self._coordinate_boundary

    def set_coordinate_boundary(self, min_latitude, max_latitude, min_longitude, max_longitude):
        """
        Set or modify the coordinate boundaries of a country.
        
        Parameters
        ----------
        value:list
            The coordinate boundary of a country.
            The format of the input is : [minimum latitude, maximum latitude,
            minimum longitude, maximum longitude]
        
        Examples
        --------
        >>> australia = GeoHierarchy(country,"Australia")
        >>> australia.set_coordinate_boundary(-43.58301104, -9.23000371,
                                              96.82159219, 167.99384663)
        >>> australia.coordinate_boundary
        [-43.58301104, -9.23000371, 96.82159219, 167.99384663]
        """
        
        if min_latitude > max_latitude:
            raise ValueError(
                "The latitute range is invalid (lat_max >= lat_min)"
            )
        
        if min_longitude > max_longitude:
            raise ValueError(
                "The longitude range is invalid (long_max >= long_min)"
            )
            
        self._coordinate_boundary = [min_latitude, max_latitude, min_longitude, max_longitude]
    
    def add_type(self, region, type_id, type_name=""):
        """
        Add a new geographical hierarchy type, for instace: statistical area,
        administrative level

        Parameters
        ----------
        region:Region
            The smallest root region for the hierarchy.
            The common upper-level region name, shared with other types of
            hierarchies, can't be assigned as a root.
            For instance, administrative level and statistical area use
            Country or State/Province as their upper regional level
        type_id:string
            The unique identifier for the structural type
        type_name:string
            The name of the regional structure type

        Examples
        --------
        >>> country = Region("Country")
        >>> state = Region("State",col_name="STATE")
        >>> sa4 = Region("Statistica Area 4",short_name="SA4",col_name="SA4")
        >>> australia = GeoHierarchy(country,"Australia")
        >>> australia.add_region(region=state, parent_region=country)
        >>> australia.add_region(region=sa4, parent_region=state)
        >>> australia.add_type(sa4,"ASGS","Australian Statistical Geography Standard")
        >>> australia.type
        {'ASGS': 'Australian Statistical Geography Standard'}
        """

        # validate the arguments
        if type_id.strip() == "":
            raise ValueError("Type id cannot be empty")

        if not isinstance(region, Region):
            raise ValueError("`region` must be a Region")

        if type_id in self._types:
            raise ValueError(f"Duplicate type ({type_id}) found")

        if type_name.strip() != "":
            self._types[type_id] = type_name
        else:
            self._types[type_id] = type_id

        self._type_root[type_id] = region

    def add_region(self, region, parent_region):
        """
        Add a region as a child/sub region of another region
        
        Parameters
        ----------
        region:Region
            The sub region to be added as a child of the parent region
        parent_region:Region
            The direct upper-level of the region
        
        Examples
        --------
        >>> country = Region("Country")
        >>> state = Region("State",col_name="STATE")
        >>> australia = GeoHierarchy(country,"Australia")
        >>> australia.add_region(region=state, parent_region=country)
        """

        # validate the arguments
        if not isinstance(region, Region):
            raise ValueError("`region` must be a Region")

        if not isinstance(parent_region, Region):
            raise ValueError("`parent_region` must be a Region")

        # if the parent region is the root of the hierarchy (country)
        # then add the new region as a sub region of the root
        if parent_region == self._country.region:
            self._country.add_child(self._Node(region))
        else:
            # find the node related to the parent_region
            parent_node = self._country.get_node_by_region(parent_region)
            if parent_node is not None:
                parent_node.add_child(self._Node(region))
            else:
                raise ValueError(f"Parent {str(parent_region)} is not found")

    def get_regions_by_name(self, region_names=None, operator=None, attribute=None):
        """
        Get all the relevant regions from the hierarchy based on the given parameters

        Parameters
        ----------
        region_names:string or list
            fill the name or short name or list of names or short names 
            of the regions in relations to operator parameter above.
            If no region names provided, the function will return all 
            regions in the hierarchy.
        operator : Operator
            use the operator to find all the upper/lower level regions
            from a particular region name.
            For instance: Country > State (Country gt State).
                          Use the 'gt' operator to search for the upper level of State
        attribute:string
            the region's attribute name that will be saved into the list
            (name, short_name, or col_name)
            if it's empty, then the list will store the object of the region

        Returns
        -------
        list
            list of regions or region's attribute. The function will return
            an empty list if there corresponding regions with the given
            name or short name are found.

        Examples
        --------
        >>> country = Region("Country")
        >>> state = Region("State",col_name="STATE")
        >>> sa4 = Region("Statistica Area 4",short_name="SA4",col_name="SA4")
        >>> australia = GeoHierarchy(country,"Australia")
        >>> australia.add_region(region=state, parent_region=country)
        >>> australia.add_region(region=sa4, parent_region=state)
        >>> regions = australia.get_regions_by_name()
        >>> for region in regions:
        >>>     print(region.name)
        Country
        State
        Statistica Area 4
        >>> regions = australia.get_regions_by_name(region_names='State', operator=le,)
        >>> for region in regions:
        >>>     print(region.name)
        State
        Statistica Area 4
        >>> col_names = australia.get_regions_by_name(region_names=['State','SA4'], attribute='col_name')
        >>> for col_name in col_names:
        >>>     print(col_name)
        STATE
        SA4
        """

        # validate the arguments
        if operator is not None:
            if operator not in [le, ge]:
                raise ValueError("Invalid operator value. Select one of 'le' or 'ge'")
        
        if operator is not None:
            if isinstance(region_names,list):
                raise ValueError(
                    "Operator has to be empty (None) if the list of `region names` is provided. vice versa."
                )
            
            if (region_names is None) or (not region_names.strip()):
                raise ValueError(
                    "One `region_name` must be provided if operator is not empty (None). vice versa."
                )
                    
        if isinstance(region_names,list):
            if len(region_names) < 1:
                raise ValueError(
                    "List of regions must have at least one element"
                )

        if attribute is not None:
            if attribute not in ["name", "short_name", "col_name"]:
                raise ValueError(
                    "Invalid attribute value. Select one of name, short_name, or col_name"
                )

        regions = []
        # get multiple region. names are provided
        if (isinstance(region_names,list)) or (region_names and operator is None):
            if not isinstance(region_names,list):
                region_names = [region_names]
                
            for reg_name in region_names:
                node = self._country.get_node_by_name(reg_name)
                if node is not None:
                    if (attribute is None) or (not attribute.strip()):
                        regions.append(node.region)
                    else:
                        regions.append(getattr(node.region, attribute))
                else:
                    ValueError("Region is not found")
        # get all the corresponding regions based on the operator 'ge' or 'le'
        elif region_names and operator is not None:
            reference_node = self._country.get_node_by_name(region_names)
            if reference_node is not None:
                if operator == ge:
                    nodes = self._country.get_all_nodes(
                        lowest_region=reference_node.region
                    )
                else:
                    nodes = reference_node.get_all_nodes()
                
                if (attribute is None) or (not attribute.strip()):
                    for node in nodes:
                        regions.append(node.region)
                else:
                    for node in nodes:
                        regions.append(getattr(node.region, attribute))
            else:
                ValueError("Region is not found")
        # get all the nodes
        else:
            nodes = self._country.get_all_nodes()
            if (attribute is None) or (not attribute.strip()):
                for node in nodes:
                    regions.append(node.region)
            else:
                for node in nodes:
                    regions.append(getattr(node.region, attribute))

        return regions

    def get_smallest_region_boundaries(self):
        """
        Get the smallest regional unit

        Returns
        -------
        Region
            the smallest regional unit

        Examples
        --------
        >>> country = Region("Country")
        >>> state = Region("State",col_name="STATE")
        >>> sa4 = Region("Statistica Area 4",short_name="SA4",col_name="SA4")
        >>> australia = GeoHierarchy(country,"Australia")
        >>> australia.add_region(region=state, parent_region=country)
        >>> australia.add_region(region=sa4, parent_region=state)
        >>> australia.get_smallest_region_boundaries().name
        Statistica Area 4
        """
        return self._country.get_leaf_node().region

    class _Node:
        """
        The internal class for the hierarchy to encapsulate the Region
        instances into a Node for building the tree purposes.
        One Node contains one Region only.
        """

        __slots__ = ("_region", "_children")

        def __init__(self, region, children=None):
            self._region = region

            if children is not None:
                self._children = children
            else:
                self._children = []

        @property
        def region(self):
            """
            Return the region within the node
            """
            return self._region

        @property
        def children(self):
            """
            Return the list of the child node
            (similar to sub-region)
            """
            return self._children

        def add_child(self, child):
            """
            Add a child to the node
            (add sub-region to a region)
            """
            self._children.append(child)

        def get_node_by_region(self, region):
            """
            Searching for a node within its tree (the node itself and
            its subnode), based on the region's object.
            """
            if self._region == region:
                return self
            else:
                for child in self._children:
                    match = child.get_node_by_region(region)
                    if match is not None:
                        return match

            # print("Node "+str(region)+" is not found")
            return None

        def get_node_by_name(self, name):
            """
            Searching for a node within its tree (the node itself and
            its subnode), based on the region's name or short name.
            """
            if (
                self._region.name.lower() == name.lower()
                or self._region.short_name.lower() == name.lower()
            ):
                return self
            else:
                for child in self._children:
                    match = child.get_node_by_name(name)
                    if match is not None:
                        return match

            return None

        def get_all_nodes(self, lowest_region=None):

            nodes = []
            if len(self._children) == 0:
                return [self]
            elif self._region == lowest_region:
                return [self]
            else:
                nodes += [self]
                for child in self._children:
                    if child not in nodes:
                        nodes += child.get_all_nodes(lowest_region)

            return nodes

        def get_leaf_node(self):
            if len(self._children) == 0:
                return self
            else:
                for child in self._children:
                    match = child.get_leaf_node()
                    if match is not None:
                        return match

        def __str__(self):
            return str(self._region + ". child:" + str(len(self._children)))

        def __eq__(self, other):
            return self._region == other._region
