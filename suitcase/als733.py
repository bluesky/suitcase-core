import uuid
import pytz
import itertools
import fabio
from fabio import fabioutils, edfimage
from collections import OrderedDict
import os

from datetime import datetime
import time
import re

import numpy as np

from filestore.handlers_base import HandlerBase
# start -> start document
# event -> baseline reading
# scatter_event -> merge with detector data
# descriptor -> camera configuration
_ALS_KEY_MAP = {
                'ABS(Vertical Beam Position)': 'event',
                'AI Channel 6': 'scatter_event',
                'AI Channel 7': 'scatter_event',
                'AIs': 'event',
                'AO Waveform': 'event',
                'Alpha_scan_I0_intensities': 'event',
                'Alpha_scan_I1_intensities': 'event',
                'Alpha_scan_diode_intensities': 'event',
                'Alpha_scan_positions': 'event',
                'Beam Current Over Threshold': 'scatter_event',
                'Beam Current': 'scatter_event',
                'Beamline Pass Beam AI': 'event',
                'Beamline Pass Beam': 'event',
                'Beamline Shutter AI': 'event',
                'Beamline Shutter Closed': 'event',
                'Beamline Shutter Open': 'event',
                'Beamstop X': 'event',
                'Beamstop Y': 'event',
                'Bruker pulses': 'event',
                'ByteOrder': 'start',
                'DIOs': 'event',
                'DataType': 'start',
                'Date': 'start',
                'Detector Horizontal': 'scatter_event',
                'Detector Left Motor': 'scatter_event',
                'Detector Right Motor': 'scatter_event',
                'Detector Vertical': 'scatter_event',
                'Dim_1': 'descriptor',
                'Dim_2': 'descriptor',
                'EZ fast tension stage': 'event',
                'Exit Slit bottom': 'event',
                'Exit Slit left': 'event',
                'Exit Slit right': 'event',
                'Exit Slit top': 'event',
                'Feedback Interlock': 'scatter_event',
                'Flight Tube Horizontal': 'event',
                'Flight Tube Vertical': 'event',
                'GIWAXS beamstop X': 'event',
                'GIWAXS beamstop Y thorlabs': 'event',
                'GIWAXS beamstop Y': 'event',
                'Gate Shutter': 'event',
                'Gate': 'event',
                'GiSAXS Beamstop Counter': 'event',
                'GiSAXS Beamstop': 'event',
                'Hacked Ager Stage': 'event',
                'HeaderID': 'start',
                'I1 AI': 'scatter_event',
                'I1': 'scatter_event',
                'Image': 'scatter_event',
                'Izero AI': 'scatter_event',
                'Izero': 'scatter_event',
                'Keyless value #1': 'scatter_event',
                'Keyless value #2': 'scatter_event',
                'Keyless value #3': 'scatter_event',
                'Kramer strain data': 'event',
                'M1 Alignment Tune': 'event',
                'M1 Bend': 'event',
                'M1 Pitch': 'event',
                'M201 Feedback': 'event',
                'Mono Angle': 'event',
                'Motorized Lab Jack': 'event',
                'Motorized Lab Jack1': 'event',
                'Motors': 'event',
                'PCO Invert': 'event',
                'PHI Alignment Beamstop': 'event',
                'Pilatus 100K exp out': 'event',
                'Pilatus 1M Trigger Pulse': 'event',
                'Pilatus 300KW trigger pulse': 'event',
                'Printing motor': 'event',
                'SAXS Protector': 'event',
                'Sample Alpha Stage': 'event',
                'Sample Phi Stage': 'event',
                'Sample Rotation Stage ESP': 'event',
                'Sample Rotation Stage Miller': 'event',
                'Sample Rotation Stage': 'event',
                'Sample Thickness Stage': 'event',
                'Sample X Stage Fine': 'event',
                'Sample X Stage': 'event',
                'Sample Y Stage Arthur': 'event',
                'Sample Y Stage': 'event',
                'Sample Y Stage_old': 'event',
                'Size': 'descriptor',
                'Slit 1 in Position': 'event',
                'Slit 2 in Position': 'event',
                'Slit Bottom Good': 'event',
                'Slit Top Good': 'event',
                'Slit1 bottom': 'event',
                'Slit1 left': 'event',
                'Slit1 right': 'event',
                'Slit1 top': 'event',
                'Sum of Slit Current': 'scatter_event',
                'Temp Beamline Shutter Open': 'event',
                'VersionNumber': 'start',
                'Vertical Beam Position': 'event',
                'Xtal2 Pico 1 Feedback': 'event',
                'Xtal2 Pico 1': 'event',
                'Xtal2 Pico 2 Feedback': 'event',
                'Xtal2 Pico 2': 'event',
                'Xtal2 Pico 3 Feedback': 'event',
                'Xtal2 Pico 3': 'event',
                'count_time': 'descriptor',
                'run': 'event',
                'slit1 bottom current': 'event',
                'slit1 top current': 'event',
                'title': 'event',
}

key_type_map = {'HeaderID': 'str',
                'Image': 'int',
                'VersionNumber': 'str',
                'ByteOrder': 'str',
                'DataType': 'str',
                'Dim_1': 'int',
                'Dim_2': 'int',
                'Size': 'int',
                'Date': 'date',
                'count_time': 'float',
                'title': 'str',
                'run': 'int',
                'Keyless value #1': 'float',
                'Keyless value #2': 'float',
                'Keyless value #3': 'float',
                'Motors': 'int',
                'Sample X Stage': 'float',
                'Sample Y Stage': 'float',
                'Sample Thickness Stage': 'float',
                'Sample X Stage Fine': 'float',
                'Sample Alpha Stage': 'float',
                'Sample Phi Stage': 'float',
                'M201 Feedback': 'float',
                'M1 Pitch': 'float',
                'Sample Rotation Stage': 'float',
                'M1 Bend': 'float',
                'Detector Horizontal': 'float',
                'Detector Vertical': 'float',
                'Slit1 top': 'float',
                'Slit1 bottom': 'float',
                'Slit1 right': 'float',
                'Slit1 left': 'float',
                'Exit Slit top': 'float',
                'Exit Slit bottom': 'float',
                'Exit Slit left': 'float',
                'Exit Slit right': 'float',
                'GIWAXS beamstop X': 'float',
                'GIWAXS beamstop Y': 'float',
                'Beamstop X': 'float',
                'Beamstop Y': 'float',
                'Detector Right Motor': 'float',
                'Detector Left Motor': 'float',
                'Motorized Lab Jack': 'float',
                'M1 Alignment Tune': 'float',
                'EZ fast tension stage': 'float',
                'Motorized Lab Jack1': 'float',
                'Sample Rotation Stage ESP': 'float',
                'Printing motor': 'float',
                'GIWAXS beamstop Y thorlabs': 'float',
                'Sample Y Stage Arthur': 'float',
                'Flight Tube Horizontal': 'float',
                'Flight Tube Vertical': 'float',
                'Hacked Ager Stage': 'float',
                'Sample Rotation Stage Miller': 'float',
                'Mono Angle': 'float',
                'Xtal2 Pico 1 Feedback': 'float',
                'Xtal2 Pico 2 Feedback': 'float',
                'Xtal2 Pico 3 Feedback': 'float',
                'Xtal2 Pico 1': 'float',
                'Xtal2 Pico 2': 'float',
                'Xtal2 Pico 3': 'float',
                'Sample Y Stage_old': 'float',
                'AO Waveform': 'float',
                'DIOs': 'int',
                'SAXS Protector': 'float',
                'Beamline Shutter Closed': 'float',
                'Beam Current Over Threshold': 'float',
                'Slit 1 in Position': 'float',
                'Slit 2 in Position': 'float',
                'Temp Beamline Shutter Open': 'float',
                'Beamline Shutter Open': 'float',
                'Feedback Interlock': 'float',
                'Beamline Pass Beam': 'float',
                'Gate Shutter': 'float',
                'Bruker pulses': 'float',
                'Slit Top Good': 'float',
                'Slit Bottom Good': 'float',
                'AIs': 'int',
                'Beam Current': 'float',
                'Beamline Shutter AI': 'float',
                'Beamline Pass Beam AI': 'float',
                'slit1 bottom current': 'float',
                'slit1 top current': 'float',
                'GiSAXS Beamstop': 'float',
                'Izero AI': 'float',
                'I1 AI': 'float',
                'PHI Alignment Beamstop': 'float',
                'AI Channel 6': 'float',
                'AI Channel 7': 'float',
                'Vertical Beam Position': 'float',
                'Pilatus 1M Trigger Pulse': 'float',
                'Pilatus 300KW trigger pulse': 'float',
                'PCO Invert': 'float',
                'Gate': 'float',
                'Izero': 'float',
                'I1': 'float',
                'GiSAXS Beamstop Counter': 'float',
                'Sum of Slit Current': 'float',
                'Pilatus 100K exp out': 'float',
                'Kramer strain data': 'float',
                'ABS(Vertical Beam Position)': 'float',
                'Alpha_scan_positions': 'str',
                'Alpha_scan_I0_intensities': 'str',
                'Alpha_scan_I1_intensities': 'str',
                'Alpha_scan_diode_intensities': 'str'
                }

conversions = {'int': lambda x: int(x.strip()),
               'float': lambda x: float(x.strip()),
               'str': lambda x: x.strip(),
               'date': lambda x: x.strip()}


class ALSEDFHandler(HandlerBase):
    specs = {'ALS_EDF'}

    def __init__(self, filename):
        self._filename = filename
        self._file = None

    def _open(self):
        if self._file is None or not self._file.id:
            self._file = fabio.open(self._filename)
        return self

    def close(self):
        super().close()
        self._file = None

    def __call__(self, *args, **kwargs):
        if not self._file:
            self._open()
        return self._file.data


def register_fabioclass(cls):
    setattr(fabio.openimage, cls.__name__, cls)
    for extension in cls.extensions:
        if extension in fabioutils.FILETYPES:
            fabioutils.FILETYPES[extension].append(cls.__name__.rstrip('image'))
        else:
            fabioutils.FILETYPES[extension] = [cls.__name__.rstrip('image')]
    return cls


@register_fabioclass
class EdfImage(edfimage.EdfImage):
    extensions = ['.edf']

    def read(self, f, frame=None):
        return super(EdfImage, self).read(f, frame)

    def _readheader(self, f):
        super(EdfImage, self)._readheader(f)
        f = f.name.replace('.edf', '.txt')
        if os.path.isfile(f):
            self.header.update(self.scanparas(f))

    @staticmethod
    def scanparas(path):
        if not os.path.isfile(path):
            return dict()

        with open(path, 'r') as f:
            lines = f.readlines()

        paras = OrderedDict()

        # The 7.3.3 txt format is messy, with keyless values, and extra whitespaces

        keylesslines = 0
        for line in lines:
            cells = [_f for _f in re.split('[=:]+', line) if _f]

            key = cells[0].strip()

            if cells.__len__() == 2:
                cells[1] = cells[1].split('/')[0]
                paras[key] = cells[1].strip()
            elif cells.__len__() == 1:
                val = cells[0].strip()
                if val:
                    keylesslines += 1
                    paras['Keyless value #' + str(keylesslines)] = key

        return paras


def ingest(fnames, fs=None):
    '''Ingest an ALS scattering edf to Documents

    This attempts to separate all of the meta-data in the attributes
    of the top-level group into the Start document, the configuration
    in Descriptors, and a stand-alone event stream.

    If a FileStore instance is provided, resources and datum will
    be inserted into it and Events will have uuids in place of the images.

    If a FileStore is not provided, the Event documents will contain the image
    payloads.

    This code makes many assumptions about the keys, attributes,
    naming schemes, and localization of datetimes.  See the code for details.


    Parameters
    ----------
    fname : Path
        The file to open.

    fs : FileStore, optional
        FileStore instance to record resources / datums.

    '''
    # TODO also support Path
    if isinstance(fnames, str):
        fnames = [fnames]

    fins = [fabio.open(fname) for fname in fnames]

    # use dict getitem to handle dispatch logic
    file_header_data = []
    for fin in fins:
        header_data = {
            'start': {},
            'descriptor': {},
            'event': {},
            'scatter_event': {}}
        for k, v in fin.header.items():
            v = conversions[key_type_map.get(k, 'str')](v)
            header_data[_ALS_KEY_MAP.get(k, 'start')][k] = v
        file_header_data.append(header_data)
    del header_data
    start_md = {}
    # TODO check for conflicts and resolve better
    [start_md.update(h_md['start']) for h_md in file_header_data[::-1]]

    st_uid = str(uuid.uuid4())

    # localize the timestamp
    tz = pytz.timezone('America/Los_Angeles')
    ts = tz.localize(datetime.strptime(
        file_header_data[0]['start']['Date'],
        '%a %b %d %H:%M:%S %Y')).timestamp()
    yield 'start', {'uid': st_uid,
                    'time': ts,
                    'sample_name': os.path.basename(fnames[0]),
                    **start_md}

    # generate descriptor + event for 'baseline' measurements
    bl_desc = _gen_descriptor_from_dict(file_header_data[0]['event'],
                                        'ALS top-level group attrs')
    yield 'descriptor', {'run_start': st_uid,
                         'name': 'baseline',
                         **bl_desc}
    for hd in file_header_data:
        yield 'event', {'descriptor': bl_desc['uid'],
                        'timestamps': {k: ts for k in hd['event']},
                        'data': hd['event'],
                        'time': ts,
                        'seq_num': 1,
                        'uid': str(uuid.uuid4())}

    # guess the camera from the first file
    fin = fins[0]
    if fin.header['Dim_1'] == '1475' and fin.header['Dim_2'] == '1679':
        cam_name = 'Pilatus 2M'
    elif fin.header['Dim_1'] == '981' and fin.header['Dim_2'] == '1043':
        cam_name = 'Pilatus 1M'
    elif fin.header['Dim_1'] == '1475' and fin.header['Dim_2'] == '195':
        cam_name = 'Pilatus 300kw'
    elif fin.header['Dim_1'] == '487' and fin.header['Dim_2'] == '619':
        cam_name = 'Pilatus 300k'
    else:
        cam_name = ''
    # generate documents for slices + bk + dark

    # TODO deal with conflicts, for now just take the first one
    cam_config_data = file_header_data[0]['descriptor']
    cam_config = {cam_name: {
        'data': cam_config_data,
        'data_keys': {k: _data_keys_from_value(
            v, 'ALS top-level group attrs', '')
            for k, v in cam_config_data.items()},
        'timestamps': {k: ts for k in cam_config_data}}
    }
    shp = [int(fin.header['Dim_1']), int(fin.header['Dim_2'])]

    cam_desc = {'run_start': st_uid,
                'time': ts,
                'configuration': cam_config,
                'object_keys': {cam_name: ['image']},
                'data_keys': {
                    'image': {'dtype': 'array',
                              'shape': shp,
                              'source': 'ALS {}'.format(cam_name),
                              'object_name': cam_name,
                              }
                }
                }
    if fs is not None:
        cam_desc['data_keys']['image']['external'] = 'FILESTORE:'
        res_uids = [fs.insert_resource('ALS_EDF', fname, {})
                    for fname in fnames]
    else:
        res_uids = [None] * len(fnames)
    header_scatter_desc = _gen_descriptor_from_dict(
        file_header_data[0]['scatter_event'],
        'ALS top-level group attrs')

    for k in ('object_keys', 'data_keys', 'configuration'):
        cam_desc[k].update(header_scatter_desc[k])

    desc_uid = str(uuid.uuid4())
    yield 'descriptor', {'run_start': st_uid,
                         'name': 'primary',
                         'uid': desc_uid,
                         **cam_desc}
    for fname, res_uid, bundled_dicts in zip(
            fnames, res_uids, file_header_data):

        dset_name = fname

        if res_uid is not None:
            d_uid = str(uuid.uuid4())
            fs.insert_datum(res_uid, d_uid, {'dset_name': dset_name})
            data = {'image': d_uid}
        else:
            data = {'image': fabio.open(fname).data.squeeze()}

        header_data_for_scatter_event = bundled_dicts['scatter_event']
        data.update(header_data_for_scatter_event)

        yield 'event', {'descriptor': desc_uid,
                        'timestamps': {'image': ts},
                        'data': data,
                        'time': ts,
                        'seq_num': 1,
                        'uid': str(uuid.uuid4())}

    # use the last event timestamp as the stop time
    yield 'stop', {'run_start': st_uid,
                   'time': ts + float(fin.header['count_time']),
                   'uid': str(uuid.uuid4()),
                   'exit_status': 'success'}


def _data_keys_from_value(v, src_name, object_name):
    kind_map = {'i': 'integer',
                'f': 'number',
                'U': 'string',
                'S': 'string'}
    return {'dtype': kind_map[np.array([v]).dtype.kind],
            'shape': [],
            'source': src_name,
            'object_name': object_name}


def _gen_descriptor_from_dict(ev_data, src_name):
    data_keys = {}
    confiuration = {}
    obj_keys = {}

    for k, v in ev_data.items():
        data_keys[k] = _data_keys_from_value(v, src_name, k)
        obj_keys[k] = [k]
        confiuration[k] = {'data': {},
                           'data_keys': {},
                           'timestamps': {}}

    return {'data_keys': data_keys,
            'time': time.time(),
            'uid': str(uuid.uuid4()),
            'configuration': confiuration,
            'object_keys': obj_keys}
