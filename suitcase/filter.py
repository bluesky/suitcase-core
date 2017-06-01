import numpy as np
import six
from skimage.transform import downscale_local_mean
import copy

def make_rebinner(nbin_v, nbin_h, field):
    """
    Decorator to do data bining.

    Parameters
    ----------
    nbin_v : int
        binning number on veritcal direction
    nbin_h : int
        binning number on horizontal direction
    """
    def rebinner(name, doc):
        doc = copy.deepcopy(doc)
        if name =='descriptor':
            try:
                v = doc['data_keys'][field]
                v['shape'][0] = v['shape'][0] // nbin_v
                v['shape'][1] = v['shape'][1] // nbin_h
            except KeyError:
                pass
            yield doc
        elif name == 'event':
            for v in doc:
                try:
                    v.data[field] = downscale_local_mean(v.data[field], (nbin_v, nbin_h))
                except KeyError:
                    pass
            yield doc
        else:
            yield doc
    return rebinner
