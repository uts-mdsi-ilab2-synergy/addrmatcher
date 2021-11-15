"""
Data structure for the regional unit
"""


class Region:
    """
    The Region class represents a unit of area in the country.
    Each region has a unique name and a corresponding column
    in the reference dataset.
    """

    __slots__ = ("_name", "_short_name", "_col_name")

    def __init__(self, name, short_name="", col_name=""):
        # region name cannot be empty
        if name is None:
            raise TabError("`name` must not be None")

        self._name = name
        self._short_name = short_name
        self._col_name = col_name

    @property
    def name(self):
        """
        Return the name of the administrative area or statistical area level.

        Returns
        -------
        string
            The name of the area unit

        Examples
        --------
        The area name can be set initially when calling the constructor.
        >>> state = new Region('State',col_name='STATE')
        >>> state.name
        'State'

        """
        return self._name

    @name.setter
    def name(self, value):
        """
        Set or rename the name of the administrative area or statistical area level.

        Parameters
        ----------
        value: string
            The name of the area unit

        Examples
        --------
        >>> state.name = 'State'
        >>> state.name
        'State'
        """
        self._name = value

    @property
    def short_name(self):
        """
        Return the short name or abbreviation of the administrative area or
        statistical area level.

        Returns
        -------
        string
            The short name of the area unit

        Examples
        --------
        The area's short name can be set initially when calling the constructor.
        >>> sa1 = new Region('Statistical Area 1',short_name='SA1',col_name='SA1')
        >>> sa1.short_name
        'SA1'
        """
        return self._short_name

    @short_name.setter
    def short_name(self, value):
        """
        Set or rename the short name or abbreviation of the administrative area or
        statistical area level.

        Parameters
        ----------
        value: string
            The short name of the area unit

        Examples
        --------
        >>> sa1.short_name = 'SA-1'
        >>> sa1.short_name
        'SA-1'
        """
        self._id = value

    @property
    def col_name(self):
        """
        Return the corresponding column name of the administrative area or
        statistical area level which is stored in the reference dataset.

        Returns
        -------
        string
            The column name of the area unit

        Examples
        --------
        The area's column name can be set initially when calling the constructor.
        >>> sa2 = new Region('Statistical Area 2',short_name='SA2',col_name='SA2')
        >>> sa2.col_name
        'SA2'

        """
        return self._col_name

    @col_name.setter
    def col_name(self, value):
        """
        Set or rename the column name of the administrative area or
        statistical area level

        Parameters
        ----------
        value: string
            The short name of the area unit
        Examples
        --------
        >>> sa2.col_name = 'SA2_2016'
        >>> sa2.col_name
        'SA2_2016'
        """
        self._col_name = value

    def __str__(self):
        return self._name

    def __eq__(self, other):
        return (
            isinstance(other, Region)
            and self._short_name == other._short_name
            and self._name == other._name
        )

    def __ne__(self, other):
        return not (self == other)
