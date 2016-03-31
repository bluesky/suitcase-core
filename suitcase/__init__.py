from .hdf5 import export as hdf_export

# for backwards compatibility
from .hdf5 import export
from .hdf5 import _safe_attrs_assignment
from .hdf5 import _clean_dict

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
