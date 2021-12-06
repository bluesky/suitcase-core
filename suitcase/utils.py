

def gen_data_keys_from_dset(dset, ob_nm, dset_nm):
    '''Generate data keys from a h5py dataset

    Parameters
    ----------
    dset : DataSet
        an h5py DataSet object to extract meta-data from

    ob_nm : str
        The 'object_name' to use in the data key dictionary

    dset_nm : str
        The same of this dataset
    '''
    fname = dset.file.filename
    sname = '{}\{}'.format
    data_keys = {dset_nm: {'dtype': 'array',
                           'shape': dset.shape,
                           'source': sname(fname, dset.name),
                           'object_name': ob_nm}
                 }
    return data_keys
