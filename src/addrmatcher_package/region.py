class Region:
    """
    
    """
    
    __slots__ = ("_name", "_short_name","_col_name", "_sub_region")
    
    def __init__(self, name, short_name="", col_name=""):
        if name is None:
            raise TabError("`name` must not be None")
        
        self._name = name
        self._short_name = short_name
        
        if col_name != "":
            self._col_name = col_name
        else:
            self._col_name = name
            
        self._sub_region = []
            
    
    @property
    def name(self):
        """
        The name of the administrative area or statistical area level
        :rtype: string
        """
        return self._name
    
    @name.setter
    def name(self, value):
        self._name = value
        
    @property
    def short_name(self):
        """
        The short name or abbreviation of the administrative area or statistical area level
        :rtype: string
        """
        return self._short_name
        
    @short_name.setter
    def short_name(self, value):
        self._id = value
        
    @property
    def col_name(self):
        """
        The column name of the region in the dataset
        :rtype: string
        """
        return self._col_name
        
    @col_name.setter
    def col_name(self, value):
        self._col_name = value
            
    @property
    def sub_region(self):
        """
        The list of region's lower administrative level/statistical area
        :rtype: list
        """
        return self._sub_region
        
    @sub_region.setter
    def sub_region(self, value):
        self._sub_region = value
        
    def add_sub_region(self, region):
        """
        add a new region as the lower administrative level/statistical area
        :rtype: string
        """
        self._sub_region.append(region)
        
    def __str__(self):
        return self._name
        
    def __eq__(self, other):
        return (
            isinstance(other, Region) and
            self._short_name == other._short_name and
            self._name == other._name
        )
        
    def __ne__(self, other):
        return not (self == other)