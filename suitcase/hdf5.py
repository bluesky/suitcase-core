#-------------------------------------------------------------------------
# Copyright (c) 2015-, Brookhaven National Laboratory
#
# Distributed under the terms of the BSD 3-Clause License.
#
# The full license is in the file LICENSE, distributed with this software.
#-------------------------------------------------------------------------

from collections import Mapping
import numpy as np
import warnings
import h5py
import json
from databroker import db
from databroker.databroker import fill_event
from databroker.core import Header


def export(headers, filename, mds=db.mds,
           stream_name=None, fields=None, timestamps=True, use_uid=True):
    """
    Create hdf5 file to preserve the structure of databroker.

    Parameters
    ----------
    headers : a Header or a list of Headers
        objects retruned by the Data Broker
    filename : string
        path to a new or existing HDF5 file
    mds : db.mds, optional
        metadatastore object
    stream_name : string, optional
        None means save all the data from each descriptor, i.e., user can define stream_name as primary,
        so only data with descriptor.name == primary will be saved.
        The default is None.
    fields : list, optional
        whitelist of names of interest; if None, all are returned;
        This is consistent with name convension in databroker.
        The default is None.
    timestamps : Bool, optional
        save timestamps or not
    use_uid : Bool, optional
        Create group name at hdf file based on uid if this value is set as True.
        Otherwise group name is created based on beamline id and run id.
    """
    if isinstance(headers, Header):
        headers = [headers]
    with h5py.File(filename) as f:
        for header in headers:
            header = dict(header)
            try:
                descriptors = header.pop('descriptors')
            except KeyError:
                warnings.warn("Header with uid {header.uid} contains no "
                              "data.".format(header), UserWarning)
                continue
            if use_uid:
                top_group_name = header['start']['uid']
            else:
                top_group_name = str(header['start']['beamline_id']) + '_' + str(header['start']['scan_id'])
            group = f.create_group(top_group_name)
            _safe_attrs_assignment(group, header)
            for i, descriptor in enumerate(descriptors):
                # make sure it's a dictionary and trim any spurious keys
                descriptor = dict(descriptor)
                if stream_name:
                    if descriptor['name'] != stream_name:
                        continue
                descriptor.pop('_name', None)

                if use_uid:
                    desc_group = group.create_group(descriptor['uid'])
                else:
                    desc_group = group.create_group(descriptor['name'])

                data_keys = descriptor.pop('data_keys')

                _safe_attrs_assignment(desc_group, descriptor)

                events = list(mds.get_events_generator(descriptor))
                event_times = [e['time'] for e in events]
                desc_group.create_dataset('time', data=event_times,
                                          compression='gzip', fletcher32=True)
                data_group = desc_group.create_group('data')
                if timestamps:
                    ts_group = desc_group.create_group('timestamps')
                [fill_event(e) for e in events]

                for key, value in data_keys.items():
                    if fields is not None:
                        if key not in fields:
                            continue
                    if timestamps:
                        timestamps = [e['timestamps'][key] for e in events]
                        ts_group.create_dataset(key, data=timestamps,
                                                compression='gzip',
                                                fletcher32=True)
                    data = [e['data'][key] for e in events]
                    data = np.array(data)

                    if value['dtype'].lower() == 'string':  # 1D of string
                        data_len = len(data[0])
                        data = data.astype('|S'+str(data_len))
                        dataset = data_group.create_dataset(
                            key, data=data, compression='gzip')
                    elif data.dtype.kind in ['S', 'U']:
                        # 2D of string, we can't tell from dytpe, they are shown as array only.
                        if data.ndim == 2:
                            data_len = 1
                            for v in data[0]:
                                data_len = max(data_len, len(v))
                            data = data.astype('|S'+str(data_len))
                            dataset = data_group.create_dataset(
                                key, data=data, compression='gzip')
                        else:
                            raise('Array of str with ndim >= 3 can not be saved.')
                    else:  # save numerical data
                        dataset = data_group.create_dataset(
                            key, data=data,
                            compression='gzip', fletcher32=True)

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


def filter_fields(headers, unwanted_fields):
    """
    Filter out unwanted fields.

    Parameters
    ----------
    headers : doct.Document or a list of that
        returned by databroker object
    unwanted_fields : list
        list of str representing unwanted filed names

    Returns
    -------
    set:
        set of selected names
    """
    if isinstance(headers, Header):
        headers = [headers]
    whitelist = set()
    for header in headers:
        for descriptor in header.descriptors:
            good = [key for key in descriptor.data_keys.keys()
                    if key not in unwanted_fields]
            whitelist.update(good)
    return whitelist
