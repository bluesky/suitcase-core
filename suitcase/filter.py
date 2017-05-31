import numpy as np
import six
from skimage.transform import downscale_local_mean

def make_rebinner(nbin_v, nbin_h, field):
    def rebinner(name, doc):
        if name =='descriptor':
            for k, v in doc.items():
                if k == 'data_keys':
                    if v == field:
                        v['shape'][0] = v['shape'][0]/nbin_v
                        v['shape'][1] = v['shape'][1]/nbin_h
                yield k, v
        elif name == 'event':
            for v in doc:
                if field in v.data.keys():
                    v.data[field] = downscale_local_mean(v.data[field], (nbin_v, nbin_h))
                yield v
        else:
            return doc
    return rebinner
