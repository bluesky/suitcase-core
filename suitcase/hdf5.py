from collections import Mapping
import warnings
import h5py
import json
from metadatastore.commands import find_events
from databroker.databroker import fill_event


__version__ = "0.2.2"

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
            header = dict(header)
            try:
                descriptors = header.pop('descriptors')
            except KeyError:
                warnings.warn("Header with uid {header.uid} contains no "
                              "data.".format(header), UserWarning)
                continue
            top_group_name = header['start']['uid']
            group = f.create_group(top_group_name)
            _safe_attrs_assignment(group, header)
            for i, descriptor in enumerate(descriptors):
                # make sure it's a dictionary and trim any spurious keys
                descriptor = dict(descriptor)
                descriptor.pop('_name', None)

                desc_group = group.create_group(descriptor['uid'])

                data_keys = descriptor.pop('data_keys')
                _safe_attrs_assignment(desc_group, descriptor)

                events = list(find_events(descriptor=descriptor))
                event_times = [e['time'] for e in events]
                desc_group.create_dataset('time', data=event_times,
                                          compression='gzip', fletcher32=True)
                data_group = desc_group.create_group('data')
                ts_group = desc_group.create_group('timestamps')
                [fill_event(e) for e in events]
                for key, value in data_keys.items():
                    value = dict(value)
                    timestamps = [e['timestamps'][key] for e in events]
                    ts_group.create_dataset(key, data=timestamps,
                                            compression='gzip',
                                            fletcher32=True)
                    data = [e['data'][key] for e in events]
                    dataset = data_group.create_dataset(
                        key, data=data, compression='gzip', fletcher32=True)
                    # Put contents of this data key (source, etc.)
                    # into an attribute on the associated data set.
                    _safe_attrs_assignment(dataset, dict(value))


def _clean_dict(d):
    d = dict(d)
    for k, v in list(d.items()):
        # Store dictionaries as JSON strings.
        if isinstance(v, Mapping):
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
            node.attrs[key] = json.dumps(value)
