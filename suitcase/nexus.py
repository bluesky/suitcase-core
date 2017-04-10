#-------------------------------------------------------------------------
# Copyright (c) 2015-2017, Brookhaven National Laboratory, Argonne National Laboratory
#
# Distributed under the terms of the BSD 3-Clause License.
#
# The full license is in the file LICENSE, distributed with this software.
#-------------------------------------------------------------------------


'''
write header(s) to NeXus HDF5 file

NeXus structure::

    NXroot
        @default = name of first NXentry group
        NXentry (one for each header)
            @default = name of first NXdata group
            descriptor_name:NXlog (one for each descriptor)
                dataset (one for each data_key)
                    @axes = name of dataset_timestamps if provided
                dataset_timestamps (one for each data_key timestamps if provided)
            descriptor_name_data:NXdata (one for each descriptor)
                @signal = name of first dataset in this group
                dataset (HDF5 hard link to original dataset in NXlog group)
                dataset_timestamps (HDF5 hard link to original dataset_timestamps in NXlog group if provided)

'''

from collections import Mapping
import numpy as np
import warnings
import h5py
import json
import re
import time
import datetime
import dateutil.parser
from databroker.databroker import fill_event
from databroker.core import Header

# prefix attribute names so we do not accidentally use a NeXus reserved name
ATTRIBUTE_PREFIX = '_BlueSky_'


def pick_NeXus_safe_name(supplied):
    '''
    ensure supplied name is consistent with NeXus name recommendations
    
    :see: http://download.nexusformat.org/doc/html/datarules.html#index-2
    '''
    safe = supplied
    if '0123456789'.find(safe[0]) >= 0:
        safe = '_' + safe
    pattern = r'[A-Za-z_][\w_]*'
    while len(re.findall(pattern, safe)) > 1:
        parts = re.split('('+pattern+')', safe, maxsplit=1)
        # assume len(parts) == 3
        safe = parts[1] + '_' + parts[2][1:]
    return safe


def export(headers, filename, mds,
           stream_name=None, fields=None, timestamps=True, use_uid=True):
    """
    Create NeXus HDF5 file to preserve the structure of databroker.

    Parameters
    ----------
    headers : a Header or a list of Headers
        objects retruned by the Data Broker
    filename : string
        path to a new or existing HDF5 file
    mds : metadatastore object
        metadatastore object or alike, like db.mds from databroker
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
        # :see: http://download.nexusformat.org/doc/html/classes/base_classes/NXroot.html
        f.attrs['file_name'] = filename
        f.attrs['file_time'] = str(datetime.datetime.now())
        f.attrs['creator'] = 'https://github.com/NSLS-II/suitcase/suitcase/nexus.py'
        f.attrs['HDF5_Version'] = h5py.version.hdf5_version

        for header in headers:
            header = dict(header)
            try:
                descriptors = header.pop('descriptors')
            except KeyError:
                warnings.warn("Header with uid {header.uid} contains no "
                              "data.".format(header), UserWarning)
                continue
            
            if use_uid:
                proposed_name = header['start']['uid']
            else:
                proposed_name = str(header['start']['beamline_id']) 
                proposed_name += '_' + str(header['start']['scan_id'])
            nxentry = f.create_group(pick_NeXus_safe_name(proposed_name))
            nxentry.attrs["NX_class"] = "NXentry"
            #header.pop('_name')
            _safe_attrs_assignment(nxentry, header)   # TODO: improve this

            if f.attrs.get("default") is None:
                f.attrs["default"] = nxentry.name.split("/")[-1]

            for i, descriptor in enumerate(descriptors):
                # make sure it's a dictionary and trim any spurious keys
                descriptor = dict(descriptor)
                if stream_name:
                    if descriptor['name'] != stream_name:
                        continue
                descriptor.pop('_name', None)

                if use_uid:
                    proposed_name = descriptor['uid']
                else:
                    proposed_name = descriptor['name']
                nxlog = nxentry.create_group(
                    pick_NeXus_safe_name(proposed_name))
                nxlog.attrs["NX_class"] = "NXlog"

                data_keys = descriptor.pop('data_keys')

                _safe_attrs_assignment(nxlog, descriptor)
                
                # TODO: possible to create a useful NXinstrument group?

                nxdata = nxentry.create_group(
                    pick_NeXus_safe_name(proposed_name + '_data'))
                nxdata.attrs["NX_class"] = "NXdata"
                if nxentry.attrs.get("default") is None:
                    nxentry.attrs["default"] = nxdata.name.split("/")[-1]
                
                '''
                structure (under nxlog:NXlog):
                
                    [data_keys]
                        @axes = data_key_timestamps
                    [data_keys]_timestamps
                    time (must be renamed or converted) Is this necessary?

                :see: http://download.nexusformat.org/doc/html/classes/base_classes/NXlog.html
                '''

                events = list(mds.get_events_generator(descriptor))
                event_times = np.array([e['time'] for e in events])
                start = event_times[0]
                ds = nxlog.create_dataset(
                    'time', data=event_times-start, compression='gzip', fletcher32=True)
                ds.attrs['units'] = 's'
                datetime_string = time.asctime(time.gmtime(start))
                ds.attrs['start'] = dateutil.parser.parse(datetime_string).isoformat()

                [fill_event(e) for e in events]

                for key, value in data_keys.items():
                    if fields is not None:
                        if key not in fields:
                            continue

                    safename = pick_NeXus_safe_name(key)
                    if timestamps:
                        timestamps = [e['timestamps'][safename] for e in events]
                        ts = nxlog.create_dataset(safename+'_timestamps', data=timestamps,
                                                compression='gzip',
                                                fletcher32=True)
                        ts.attrs['key_name'] = key
                    else:
                        ts = None

                    data = [e['data'][key] for e in events]
                    data = np.array(data)

                    if value['dtype'].lower() == 'string':  # 1D of string
                        data_len = len(data[0])
                        data = data.astype('|S'+str(data_len))
                        dataset = nxlog.create_dataset(
                            safename, data=data, compression='gzip')
                    elif data.dtype.kind in ['S', 'U']:
                        # 2D of string, we can't tell from dytpe, they are shown as array only.
                        if data.ndim == 2:
                            data_len = 1
                            for v in data[0]:
                                data_len = max(data_len, len(v))
                            data = data.astype('|S'+str(data_len))
                            dataset = nxlog.create_dataset(
                                safename, data=data, compression='gzip')
                        else:
                            raise ValueError('Array of str with ndim >= 3 can not be saved.')
                    else:  # save numerical data
                        dataset = nxlog.create_dataset(
                            safename, data=data,
                            compression='gzip', fletcher32=True)
                    dataset.attrs['key_name'] = key
                    
                    # only link to the NXdata group if the data is numerical
                    if value['dtype'] in ('number',):
                        nxdata[safename] = dataset
                        dataset.attrs['target'] = dataset.name
                        if nxdata.attrs.get("h5py") is None:
                            nxdata.attrs["signal"] = dataset.name.split("/")[-1]

                    if ts is not None:
                        # point to the associated timestamp array
                        dataset.attrs['axes'] = ts.name.split('/')[-1]
                        if value['dtype'] in ('number',):
                            nxdata[dataset.attrs['axes']] = ts
                            ts.attrs['target'] = ts.name

                    # Put contents of this data key (source, etc.)
                    # into an attribute on the associated data set.
                    _safe_attrs_assignment(dataset, dict(value))

                if nxdata.attrs.get("signal") is None:
                    del nxdata  # TODO: is this the correct way to delete the empty NXdata group?


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
            node.attrs[ATTRIBUTE_PREFIX + key] = value
        # Fallback: Save the repr, which in many cases can be used to
        # recreate the object.
        except TypeError:
            node.attrs[ATTRIBUTE_PREFIX + key] = json.dumps(value)


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
