# ingest Series files into databroker (for Materials Project)
import uuid
import time
import numpy as np

from databroker import Broker, temp_config


def make_header(fname, prefix=None, suffix=None):
    ''' Make a dummy header from file if one not supplied.'''
    if prefix is None:
        prefix = ""
    if suffix is None:
        suffix = ""

    # quickly load (files assumed not to be large)
    data = np.loadtxt(fname).T
    numcols = len(data[:,0])-1

    header = [f"{prefix}{i}{suffix}" for i in range(numcols)]
    header = ["energy"] + header

    return header


def parse_series_file(fname, header=None, **kwargs):
    ''' Parse a series file which is assumed to be
        energies mu1 mu2 ....
    '''
    if header is None:
        header = make_header(fname)

    res = np.loadtxt(fname, **kwargs).T

    energies = res[0]
    data = res[1:]
    data_dict = dict()

    for i, name in enumerate(header[1:]):
        data_dict[name] = data[i]

    return energies, data_dict


def ingest(fname, header=None, md=None):
    ''' Ingest Series file.
        This file is an ascii table of Energy, ...
        where the remaining values are from samples.

        Parameters
        ----------
        fname : str
            the filename to read from (Series file)

        md : dict, optional
            additional metadata to add (author name? etc)

        header: str
            header for the columns
            (should be energy, mu1 ....)

        Returns
        -------
            Generator of documents to be inserted into databroker
    '''
    energies, data_dict = parse_series_file(fname, header=header)

    for data_key, data in data_dict.items():
        doc_time = time.time()
        start = {'uid': str(uuid.uuid4()),
                 'time': doc_time,
                 **md}
        start['composition'] = data_key

        yield 'start', start

        desc = {'uid': str(uuid.uuid4()),
                'time': doc_time,
                'run_start': start['uid'],
                'name': 'primary',
                'data_keys': {'energy' : {'dtype': 'number', 'shape': None, 'source':
                                   'Series'},
                              data_key : {'dtype': 'number', 'shape': None, 'source':
                                   'Series'}
                             }
               }

        yield 'descriptor', desc
        _ts = {data_key: doc_time, 'energy' : doc_time}
        for i in range(data.shape[0]):
            data_dict_one = {'energy' : energies[i], data_key : data[i]}
            yield 'event', {'uid': str(uuid.uuid4()),
                            'descriptor': desc['uid'],
                            'time': doc_time,
                            'seq_num': i+1,
                            'data': data_dict_one,
                            'filled': {},
                            'timestamps': _ts}

        yield 'stop', {'uid': str(uuid.uuid4()),
                       'time': doc_time,
                       'run_start': start['uid'],
                       'exit_status': 'success',
                       'num_events': {'primary': (data.shape[0])}}
