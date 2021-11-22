"""
Data structure for the regional unit
"""
from dataclasses import dataclass


@dataclass
class Region:
    """
    The Region class represents a unit of area in the country.
    Each region has a unique name and a corresponding column
    in the reference dataset.

    Parameters
    ----------
    name: string
        The name of the area unit
    short_name: string
        The short name of the area unit
    col_name: string
        The column name of the area unit

    Examples
    --------
    The area's column name can be set initially when calling the constructor.
    >>> sa2 = Region('Statistical Area 2',short_name='SA2',col_name='SA2')
    >>> sa2.col_name
    'SA2'
    """

    name: str
    short_name: str = ""
    col_name: str = ""

    def __str__(self):
        return self.name

    def __eq__(self, other):
        return (
            isinstance(other, Region)
            and self.short_name == other.short_name
            and self.name == other.name
        )

    def __ne__(self, other):
        return not (self == other)
