# ingest Series files into databroker (for Materials Project)
import uuid
import time
import numpy as np

from databroker import Broker, temp_config


def parse_series_file(fname, prefix=None, **kwargs):
    if prefix is None:
        prefix = ""

    res = np.loadtxt(fname, **kwargs).T
    energies = res[0]
    data = res[1:]
    data_dict = dict()

    for i in range(len(data)):
        key = prefix + f"{i}"
        data_dict[key] = data[i]

    return energies, data_dict


def ingest(fname, headerprefix=None, md=None):
    ''' Ingest Series file.
        This file is an ascii table of Energy, ...
        where the remaining values are from samples.

        Parameters
        ----------
        fname : str
            the filename to read from (Series file)

        md : dict, optional
            additional metadata to add (author name? etc)

        headerprefix : str
            prefix for the columns of the series file

        Returns
        -------
            Generator of documents to be inserted into databroker

    '''
    energies, data_dict = parse_series_file(fname, prefix=headerprefix)

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
                              data_key: {'dtype': 'number', 'shape': None, 'source':
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
