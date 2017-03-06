import h5py
import uuid
import pytz
import itertools

from datetime import datetime
import time
import re

import numpy as np

from filestore.handlers_base import HandlerBase


_ALS_KEY_MAP = {'scanner': 'start',
                'object': 'start',
                'archdir': 'start',
                'xtechdir': 'start',
                'pfile': 'start',
                'rfile': 'start',
                'dtype': 'start',
                'stype': 'start',
                'pgeometry': 'start',
                'pfilegeom': 'start',
                'sdate': 'start',
                'experimenter': 'start',
                'nangles': 'start',
                'nslices': 'start',
                'nrays': 'start',
                'arange': 'start',
                'pxcenter': 'descriptor',
                'pycenter': 'descriptor',
                'pzcenter': 'descriptor',
                'pxsize': 'descriptor',
                'pysize': 'descriptor',
                'pzsize': 'descriptor',
                'pxdist': 'descriptor',
                'pydist': 'descriptor',
                'pzdist': 'descriptor',
                'rxsize': 'start',
                'rysize': 'start',
                'rzsize': 'start',
                'rxdist': 'start',
                'rydist': 'start',
                'rzdist': 'start',
                'bgeometry': 'start',
                'senergy': 'event',
                'scurrent': 'event',
                'dxelements': 'start',
                'dzelements': 'start',
                'dxfov': 'start',
                'dzfov': 'start',
                'xoffset': 'start',
                'yoffset': 'start',
                'rxelements': 'start',
                'rzelements': 'start',
                'ryelements': 'start',
                'dxsize': 'start',
                'dzsize': 'start',
                'dxdist': 'start',
                'dzdist': 'start',
                'evalxstrt': 'start',
                'evalystrt': 'start',
                'evalxsize': 'start',
                'evalysize': 'start',
                'cdtype': 'descriptor',
                'cdmaterial': 'descriptor',
                'cdxsize': 'descriptor',
                'cdzsize': 'descriptor',
                'cddepth': 'descriptor',
                'obstime': 'descriptor',
                'naverages': 'descriptor',
                'xbin': 'descriptor',
                'ybin': 'descriptor',
                'cooler_on': 'descriptor',
                'cooler_target': 'descriptor',
                'diglev': 'descriptor',
                'cammode': 'descriptor',
                'ofactor': 'descriptor',
                'sfactor': 'descriptor',
                'stgsel': 'descriptor',
                'stepdeg': 'descriptor',
                'rorder': 'descriptor',
                'noisesig': 'descriptor',
                'rfilter': 'descriptor',
                'rfcutoff': 'descriptor',
                'rforder': 'descriptor',
                'i0hmove': 'descriptor',
                'i0vmove': 'descriptor',
                'i0cycle': 'descriptor',
                'tile_xnumimg': 'start',
                'tile_ynumimg': 'start',
                'tile_xorig': 'start',
                'tile_yorig': 'start',
                'tile_xmovedist': 'start',
                'tile_ymovedist': 'start',
                'tile_xoverlap': 'start',
                'tile_yoverlap': 'start',
                'scan_then_tile': 'start',
                'new_dirs_per_scan': 'start',
                'axis1pos': 'event',
                'axis2pos': 'event',
                'axis3pos': 'event',
                'axis4pos': 'event',
                'axis5pos': 'event',
                'axis6pos': 'event',
                'auto_eval_roi': 'descriptor',
                'usebrightexpose': 'descriptor',
                'brightexptime': 'descriptor',
                'phase_filt': 'descriptor',
                'remove_outliers': 'descriptor',
                'simple_ring_removal': 'descriptor',
                'normalize_by_ROI': 'descriptor',
                'Reconstruction_Type': 'descriptor',
                'ring_removal_method': 'descriptor',
                'distance': 'start',
                'beta': 'event',
                'delta': 'event',
                'radius': 'event',
                'threshold': 'descriptor',
                'kernel_size': 'descriptor',
                'exclude_selected_projections': 'descriptor',
                'normalization_ROI_left': 'descriptor',
                'normalization_ROI_right': 'descriptor',
                'normalization_ROI_top': 'descriptor',
                'normalization_ROI_bottom': 'descriptor',
                'output_type': 'descriptor',
                'output_scaling_min_value': 'descriptor',
                'output_scaling_max_value': 'descriptor',
                'low_ring_value': 'descriptor',
                'upp_ring_value': 'descriptor',
                'max_ring_size': 'descriptor',
                'max_arc_length': 'descriptor',
                'ring_threshold': 'descriptor',
                'camera_used': 'start',
                'projection_mode': 'start',
                'num_dark_fields': 'start',
                'dark_num_avg_of': 'start',
                'num_bright_field': 'start',
                'bright_num_avg_of': 'start',
                'frsum': 'start',
                'time_elapsed': 'descriptor',
                'time_stamp': 'start',
                'TC0': 'event',
                'TC1': 'event',
                'TC2': 'event',
                'TC3': 'event',
                'Mono_Energy': 'event',
                'Beam_Current': 'event',
                'Izero': 'event',
                'Dark_Offset': 'descriptor',
                'optics_type': 'descriptor',
                'lens_name': 'descriptor',
                'scintillator_name': 'descriptor',
                'tilt': 'event',
                'nhalfCir': 'start',
                'multiRev': 'start',
                'Lead_Flag': 'event',
                'turret1': 'event',
                'turret2': 'event',
                'Z2': 'event',
                'Horiz_Slit_A_Wall': 'event',
                'Horiz_Slit_A_Door': 'event',
                'Horiz_Slit_Pos': 'event',
                'Horiz_Slit_Size': 'event',
                'Filter_Motor': 'event',
                'postImageDelay': 'descriptor',
                'Camera_Z_Support': 'event',
                'blur_limit': 'descriptor'}


key_type_map = {
    'scanner': 'str',
    'object': 'str',
    'archdir': 'str',
    'xtechdir': 'str',
    'pfile': 'str',
    'rfile': 'str',
    'dtype': 'str',
    'stype': 'str',
    'pgeometry': 'str',
    'pfilegeom': 'str',
    'sdate': 'date',
    'experimenter': 'str',
    'nangles': 'int',
    'nslices': 'int',
    'nrays': 'int',
    'arange': 'float',
    'pxcenter': 'float',
    'pycenter': 'float',
    'pzcenter': 'float',
    'pxsize': 'float',
    'pysize': 'float',
    'pzsize': 'float',
    'pxdist': 'float',
    'pydist': 'float',
    'pzdist': 'float',
    'rxsize': 'float',
    'rysize': 'float',
    'rzsize': 'float',
    'rxdist': 'float',
    'rydist': 'float',
    'rzdist': 'float',
    'bgeometry': 'str',
    'senergy': 'float',
    'scurrent': 'float',
    'dxelements': 'int',
    'dzelements': 'int',
    'dxfov': 'float',
    'dzfov': 'float',
    'xoffset': 'float',
    'yoffset': 'float',
    'rxelements': 'int',
    'rzelements': 'int',
    'ryelements': 'int',
    'dxsize': 'str',
    'dzsize': 'str',
    'dxdist': 'float',
    'dzdist': 'float',
    'evalxstrt': 'float',
    'evalystrt': 'float',
    'evalxsize': 'float',
    'evalysize': 'float',
    'cdtype': 'str',
    'cdmaterial': 'str',
    'cdxsize': 'str',
    'cdzsize': 'str',
    'cddepth': 'float',
    'obstime': 'float',
    'naverages': 'int',
    'xbin': 'int',
    'ybin': 'int',
    'cooler_on': 'int',
    'cooler_target': 'float',
    'diglev': 'int',
    'cammode': 'int',
    'ofactor': 'float',
    'sfactor': 'float',
    'stgsel': 'int',
    'stepdeg': 'float',
    'rorder': 'int',
    'noisesig': 'float',
    'rfilter': 'str',
    'rfcutoff': 'float',
    'rforder': 'float',
    'i0hmove': 'float',
    'i0vmove': 'float',
    'i0cycle': 'int',
    'tile_xnumimg': 'int',
    'tile_ynumimg': 'int',
    'tile_xorig': 'float',
    'tile_yorig': 'float',
    'tile_xmovedist': 'float',
    'tile_ymovedist': 'float',
    'tile_xoverlap': 'float',
    'tile_yoverlap': 'float',
    'scan_then_tile': 'int',
    'new_dirs_per_scan': 'int',
    'axis1pos': 'float',
    'axis2pos': 'float',
    'axis3pos': 'float',
    'axis4pos': 'float',
    'axis5pos': 'float',
    'axis6pos': 'float',
    'auto_eval_roi': 'int',
    'usebrightexpose': 'int',
    'brightexptime': 'float',
    'phase_filt': 'int',
    'remove_outliers': 'int',
    'simple_ring_removal': 'int',
    'normalize_by_ROI': 'int',
    'Reconstruction_Type': 'str',
    'ring_removal_method': 'str',
    'distance': 'float',
    'beta': 'float',
    'delta': 'float',
    'radius': 'float',
    'threshold': 'float',
    'kernel_size': 'float',
    'exclude_selected_projections': 'float',
    'normalization_ROI_left': 'float',
    'normalization_ROI_right': 'float',
    'normalization_ROI_top': 'float',
    'normalization_ROI_bottom': 'float',
    'output_type': 'float',
    'output_scaling_min_value': 'float',
    'output_scaling_max_value': 'float',
    'low_ring_value': 'float',
    'upp_ring_value': 'float',
    'max_ring_size': 'float',
    'max_arc_length': 'float',
    'ring_threshold': 'float',
    'camera_used': 'str',
    'projection_mode': 'str',
    'num_dark_fields': 'int',
    'dark_num_avg_of': 'int',
    'num_bright_field': 'int',
    'bright_num_avg_of': 'int',
    'frsum': 'int',
    'time_elapsed': 'float',
    'time_stamp': 'date',
    'TC0': 'float',
    'TC1': 'float',
    'TC2': 'float',
    'TC3': 'float',
    'Mono_Energy': 'float',
    'Beam_Current': 'float',
    'Izero': 'float',
    'Dark_Offset': 'float',
    'optics_type': 'str',
    'lens_name': 'str',
    'scintillator_name': 'str',
    'tilt': 'float',
    'nhalfCir': 'int',
    'multiRev': 'int',
    'Lead_Flag': 'float',
    'turret1': 'float',
    'turret2': 'float',
    'Z2': 'float',
    'Horiz_Slit_A_Wall': 'float',
    'Horiz_Slit_A_Door': 'float',
    'Horiz_Slit_Pos': 'float',
    'Horiz_Slit_Size': 'float',
    'Filter_Motor': 'float',
    'postImageDelay': 'float',
    'Camera_Z_Support': 'float',
    'blur_limit': 'float',
}


class ALSHDF5Handler(HandlerBase):
    specs = {'ALS_HDF'}

    def __init__(self, filename, group):
        self._filename = filename
        self._group_nm = group
        self._file = None
        self._group = None

    def _open(self):
        if self._file is None or not self._file.id:
            self._file = h5py.File(self._filename, 'r')
            self._group = self._file[self._group_nm]

    def close(self):
        super().close()
        self._group = None
        self._file.close()
        self._file = None

    def __call__(self, dset_name):
        if not self._group:
            self._open()
        return self._group[dset_name][:].squeeze()

    def get_file_list(self, datum_kwarg_gen):
        return [self._filename]


conversions = {'int': lambda x: int(x.decode().strip()),
               'float': lambda x: float(x.decode().strip()),
               'str': lambda x: x.decode().strip(),
               'date': lambda x: x.decode().strip()}


def ingest(fname, fs=None):
    '''Injest an ALS tomography to Documents

    This attempts to separate all of the meta-data in the attributes
    of the top-level group into the Start document, the configuration
    in Descriptors, and a stand-alone event stream.

    If a FileStore instance is provided, resources and datum will
    be inserted into it and Events will have uuids in place of the images.

    If a FileStore is not provided, the Event documents will contain the image
    payloads.

    This code makes many assumptions about the keys, attributes,
    naming schemes, and localization of datetimes.  See the code for details.

    This will generate 4 event streams:

      - primary : the tomographic data
      - baseline : measurements extracted from attrs on the top level group
      - background : background images
      - darkfield : darkfield images


    Parameters
    ----------
    fname : Path
        The file to open.

    fs : FileStore, optional
        FileStore instance to record resources / datums.

    '''
    with h5py.File(fname) as fin:
        for g_name, grp in fin.items():
            # use dict getitem to handle dispatch logic
            bundled_dicts = {
                'start': {},
                'descriptor': {},
                'event': {}}
            for k, v in grp.attrs.items():
                v = conversions[key_type_map.get(k, 'string')](v)
                bundled_dicts[_ALS_KEY_MAP.get(k, 'start')][k] = v
            st_uid = str(uuid.uuid4())

            # localize the timestamp
            tz = pytz.timezone('America/Los_Angeles')
            ts = tz.localize(datetime.strptime(
                bundled_dicts['start']['sdate'],
                '%m-%d-%Y %H:%M:%S')).timestamp()
            yield 'start', {'uid': st_uid,
                            'time': ts,
                            **bundled_dicts['start']}

            base_name = bundled_dicts['start']['object']

            # generate descriptor + event for 'baseline' measurements
            bl_ev_data = bundled_dicts['event']
            bl_desc = _gen_descriptor_from_dict(bl_ev_data,
                                                'ALS top-level group attrs')
            yield 'descriptor', {'run_start': st_uid,
                                 'name': 'baseline',
                                 **bl_desc
                                 }

            yield 'event', {'descriptor': bl_desc['uid'],
                            'timestamps': {k: ts for k in bl_ev_data},
                            'data': bundled_dicts['event'],
                            'time': ts,
                            'seq_num': 1,
                            'uid': str(uuid.uuid4())}

            # generate documents for slices + bk + dark
            cam_name = bundled_dicts['start']['camera_used']
            cam_config_data = bundled_dicts['descriptor']
            cam_config = {cam_name: {
                'data': cam_config_data,
                'data_keys': {k: _data_keys_from_value(
                    v, 'ALS top-level group attrs', '')
                              for k, v in cam_config_data.items()},
                'timestamps': {k: ts for k in cam_config_data}}
                          }
            shp = [bundled_dicts['start']['nslices'],
                   bundled_dicts['start']['nrays']]

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
                cam_desc['data_keys']['image']['external'] = 'FILESTORE'
                res_uid = fs.insert_resource('ALS_HDF', fname,
                                             {'group': g_name})
            else:
                res_uid = None

            primary_uid = str(uuid.uuid4())
            bck_uid = str(uuid.uuid4())
            drk_uid = str(uuid.uuid4())
            for nm, uid in zip(('primary', 'background', 'darkframe'),
                               (primary_uid, bck_uid, drk_uid)):
                yield 'descriptor', {'run_start': st_uid,
                                     'name': nm,
                                     'uid': uid,
                                     **cam_desc}
            c = re.compile(
                '{}(drk|bak|)_[0-9]{{4}}_[0-9]{{4}}\.tif'.format(base_name))
            uid_map = {'': primary_uid,
                       'drk': drk_uid,
                       'bak': bck_uid}

            stop_ts = 0

            counters = {k: itertools.count(1) for k in uid_map}
            tz = pytz.timezone('UTC')
            for dset_name, dset in grp.items():
                s_type = c.match(dset_name).groups()[0]
                if res_uid is not None:
                    d_uid = str(uuid.uuid4())
                    fs.insert_datum(res_uid, d_uid, {'dset_name': dset_name})
                    data = {'image': d_uid}
                else:
                    data = {'image': dset[:].squeeze()}
                ev_ts = tz.localize(
                    datetime.strptime(dset.attrs['date'].decode(),
                                      '%Y-%m-%dT%H:%MZ')).timestamp()
                if ev_ts > stop_ts:
                    stop_ts = ev_ts
                yield 'event', {
                    'descriptor': uid_map[s_type],
                    'data': data,
                    'timestamps': {'image': ev_ts},
                    'uid': str(uuid.uuid4()),
                    'seq_num': next(counters[s_type]),
                    'time': ev_ts
                }
            # use the last event timestamp as the stop time
            yield 'stop', {'run_start': st_uid,
                           'time': stop_ts,
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
