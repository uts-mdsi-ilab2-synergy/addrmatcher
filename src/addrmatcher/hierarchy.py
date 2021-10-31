from .region import Region
from operator import le, ge


class GeoHierarchy:
    """
    The GeoHierarchy class represents the structure of a country's region/area;
    for instance, a state or a province is the sub-region of a country.
    """

    __slot__ = (
        "_type",
        "_type_root",
        "_country",
        "_country_name",
        "_coordinate_boundary",
    )

    def __init__(self, country, name, coordinate_boundary=None):
        # country is the root of the hierarchy
        self._type = {}
        self._type_root = {}
        self._country = self._Node(country)
        self._country_name = name
        self.coordinate_boundary = coordinate_boundary

    @property
    def type(self):
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
        >>> australia = new GeoHierarchy(country,"Australia")
        >>> australia.add_region(region=state, parent_region=country)
        >>> australia.add_region(region=sa4, parent_region=state)
        >>> australia.add_type(sa4,"ASGS","Australian Statistical Geography Standard")
        >>> australia.type
        {'ASGS': 'Australian Statistical Geography Standard'}
        """
        return self._type

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
        >>> australia = new GeoHierarchy(country,"Australia")
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
        >>> australia = new GeoHierarchy(country,"Australia")
        >>> australia.coordinate_boundary = [-43.58301104, -9.23000371,
                                             96.82159219, 167.99384663]
        >>> australia.coordinate_boundary
        [-43.58301104, -9.23000371, 96.82159219, 167.99384663]
        """
        return self._coordinate_boundary

    @coordinate_boundary.setter
    def coordinate_boundary(self, value):
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
        >>> australia = new GeoHierarchy(country,"Australia")
        >>> australia.coordinate_boundary = [-43.58301104, -9.23000371,
                                             96.82159219, 167.99384663]
        >>> australia.coordinate_boundary
        [-43.58301104, -9.23000371, 96.82159219, 167.99384663]
        """
        if value is not None:
            if isinstance(value, list) and len(value):
                if (value[0] < value[1]) and (value[2] < value[3]):
                    self._coordinate_boundary = value
                else:
                    if value[0] >= value[1]:
                        raise ValueError(
                            "The latitute range is invalid (lat_max >= lat_min)"
                        )
                    else:
                        raise ValueError(
                            "The longitude range is invalid (long_max >= long_min)"
                        )
            else:
                raise ValueError(
                    "The boundary must be a four element list "
                    "[lat_min, lat_max, long_min, long_max]"
                )

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
        >>> australia = new GeoHierarchy(country,"Australia")
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

        if type_id in self._type:
            raise ValueError("Duplicate type (" + type_id + ") found")

        if type_name.strip() != "":
            self._type[type_id] = type_name
        else:
            self._type[type_id] = type_id

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
        >>> australia = new GeoHierarchy(country,"Australia")
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
                raise ValueError(str(parent_region) + " is not found")

    def get_regions_by_name(self, operator=None, name="", names=[], attribute=""):
        """
        Get all the relevant regions from the hierarchy based on the given parameters
        Parameters
        ----------
        operator: Operator
            use the operator to find all the upper/lower level regions
            from a particular region name.
            For instance: Country > State (Country gt State).
                          Use the 'gt' operator to search for the upper level of State
        name:string
            fill the name or short name of the regions in relations to operator parameter above
        names:list
            the list of region's name to look for
        attribute:string
            the region's attribute name that will be saved into the list
            (name, short_name, or col_name)
            if it's empty, then the list will store the object of the region
        Returns
        -------
        list
            list of regions or region's attribute
        Examples
        --------
        >>> country = Region("Country")
        >>> state = Region("State",col_name="STATE")
        >>> sa4 = Region("Statistica Area 4",short_name="SA4",col_name="SA4")
        >>> australia = new GeoHierarchy(country,"Australia")
        >>> australia.add_region(region=state, parent_region=country)
        >>> australia.add_region(region=sa4, parent_region=state)
        >>> regions = australia.get_regions_by_name()
        >>> for region in regions:
        >>>     print(region.name)
        Country
        State
        Statistica Area 4
        >>> regions = australia.get_regions_by_name(operator=le,name='State')
        >>> for region in regions:
        >>>     print(region.name)
        State
        Statistica Area 4
        >>> col_names = australia.get_regions_by_name(names=['State','SA4'], attribute='col_name')
        >>> for col_name in col_names:
        >>>     print(col_name)
        STATE
        SA4
        """

        # validate the arguments
        if operator is not None and len(names) > 0:
            raise ValueError(
                "Operator has to be empty if list of `names` is provided. vice versa."
            )

        if operator is not None and name.strip() == "":
            raise ValueError(
                "`name` must be provided if operator is not empty. vice versa."
            )

        if operator is not None:
            if operator not in [le, ge]:
                raise ValueError("Invalid operator value. Select one of 'le' or 'ge'")

        if attribute.strip() != "":
            if attribute not in ["name", "short_name", "col_name"]:
                raise ValueError(
                    "Invalid attribute value. Select one of name, short_name, or col_name"
                )

        regions = []
        # get multiple region. names are provided
        if len(names) > 0:
            for reg_name in names:
                node = self._country.get_node_by_name(reg_name)
                if node is not None:
                    if attribute.strip() == "":
                        regions.append(node.region)
                    else:
                        regions.append(getattr(node.region, attribute))
                else:
                    ValueError("Region is not found")
        # get all the corresponding regions based on the operator 'ge' or 'le'
        elif name != "":
            reference_node = self._country.get_node_by_name(name)
            if reference_node is not None:
                if operator in [ge]:
                    nodes = self._country.get_all_nodes(
                        lowest_region=reference_node.region
                    )
                    if attribute.strip() == "":
                        for node in nodes:
                            regions.append(node.region)
                    else:
                        for node in nodes:
                            regions.append(getattr(node.region, attribute))
                else:
                    nodes = reference_node.get_all_nodes()
                    if attribute.strip() == "":
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
            if attribute.strip() == "":
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
        >>> australia = new GeoHierarchy(country,"Australia")
        >>> australia.add_region(region=state, parent_region=country)
        >>> australia.add_region(region=sa4, parent_region=state)
        >>> australia.get_smallest_region_boundaries.name
        Statistica Area 4
        """
        return self._country.get_leaf_node().region

    class _Node:
        """
        The internal class for the hierarchy to encapsulate the Region
        instances into a Node for building the tree purposes.
        One Node contains one Region only.
        """

        __slot__ = ("_region", "_child")

        def __init__(self, region, child=[]):
            self._region = region

            if len(child) > 0:
                self._child = child
            else:
                self._child = []

        @property
        def region(self):
            """
            Return the region within the node
            """
            return self._region

        @property
        def child(self):
            """
            Return the list of the child node
            (similar to sub-region)
            """
            return self._child

        def add_child(self, child):
            """
            Add a child to the node
            (add sub-region to a region)
            """
            self._child.append(child)

        def get_node_by_region(self, region):
            """
            Searching for a node within its tree (the node itself and
            its subnode), based on the region's object.
            """
            if self._region == region:
                return self
            else:
                for child in self._child:
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
                for child in self._child:
                    match = child.get_node_by_name(name)
                    if match is not None:
                        return match

            return None

        def get_all_nodes(self, lowest_region=None):

            nodes = []
            if len(self._child) == 0:
                return [self]
            elif self._region == lowest_region:
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
            return str(self._region + ". child:" + str(len(self._child)))

        def __eq__(self, other):
            return self._region == other._region
