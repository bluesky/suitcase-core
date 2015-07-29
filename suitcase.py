import copy
from collections import MutableMapping
import warnings
import h5py
import json
import numpy as np
import metadatastore
from metadatastore.commands import find_events
from dataportal.broker.simple_broker import fill_event


def export(headers, filename):
    """
    Parameters
    ----------
    headers : a Header or a list of Headers
        objects retruned by the Data Broker
    filename : string
        path to a new or existing HDF5 file
    """
    with h5py.File(filename) as f:
        for header in headers:
            assert isinstance(header, MutableMapping)
            header = copy.deepcopy(header)
            try:
                descriptors = header.pop('event_descriptors')
            except KeyError:
                warnings.warn("Header with uid {header.uid} contains no "
                              "data.".format(header), UserWarning)
                continue
            top_group_name = repr(header).replace(' ', '_')[1:-1]
            group = f.create_group(top_group_name)
            _safe_attrs_assignment(group, header)
            for i, descriptor in enumerate(descriptors):
                desc_group = group.create_group('Event_Stream_{0}'.format(i))
                data_keys = descriptor.pop('data_keys')
                _safe_attrs_assignment(desc_group, descriptor)
                events = list(find_events(descriptor=descriptor))
                event_times = [e['time'] for e in events]
                desc_group.create_dataset('event_times', event_times)
                data_group = desc_group.create_group('data')
                ts_group = desc_group.create_group('timestamps')
                [fill_event(e) for e in events]
                for key, value in data_keys.items():
                    print('data key = %s' % key)
                    timestamps = [e['timestamps'][key] for e in events]
                    ts_group.create_dataset(key, data=timestamps)
                    data = [e['data'][key] for e in events]
                    dataset = data_group.create_dataset(key, data=data)
                    # Put contents of this data key (source, etc.)
                    # into an attribute on the associated data set.
                    # value['data_broker_shape'] = value.pop('shape')
                    # value['data_broker_dtype'] = value.pop('dtype')
                    # _safe_attrs_assignment(dataset, dict(value))


def _clean_dict(d):
    for k, v in list(d.items()):
        # Store dictionaries as JSON strings.
        if isinstance(v, MutableMapping):
            d[k] = _clean_dict(d[k])
            continue
        try:
            json.dumps(v)
        except TypeError:
            d[k] = str(v)
    return d


def _safe_attrs_assignment(node, d):
    d = _clean_dict(d)
    for key, value in d.items():
        # Special-case None, which fails too late to catch below.
        if value is None:
            value = 'None'
        # Try storing natively.
        try:
            node.attrs[key] = value
        # Fallback: Save the repr, which in many cases can be used to
        # recreate the object.
        except TypeError:
            node.attrs[key] = repr(value)
