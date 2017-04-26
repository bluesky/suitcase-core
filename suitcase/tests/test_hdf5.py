from metadatastore.examples.sample_data import temperature_ramp
from databroker import Broker
from suitcase import hdf5
import tempfile
import h5py
import numpy as np
import pytest


def shallow_header_verify(hdf_path, header, fields=None,
                          stream_name=None, use_uid=True, db=None):
    with h5py.File(hdf_path) as f:
        # make sure that the header is actually in the file that we think it is
        # supposed to be in
        if use_uid:
            sub_path = header.start.uid
        else:
            sub_path = header.start.beamline_id + '_' + str(header.start.scan_id)
        assert sub_path in f
        assert dict(header.start) == eval(f[sub_path].attrs['start'])
        assert dict(header.stop) == eval(f[sub_path].attrs['stop'])

        # make sure the descriptors are all in the hdf output file
        for descriptor in header.descriptors:
            if stream_name is not None:
                if stream_name != descriptor.name:
                    continue
            table_load = db.mds.get_events_table(descriptor)
            table = table_load[1]  # only get data
            if use_uid:
                descriptor_path = '%s/%s' % (sub_path, descriptor.uid)
                assert descriptor_path in f
            else:
                descriptor_path = '%s/%s' % (sub_path, descriptor.name)
                assert descriptor_path in f
            # make sure all keys are in each descriptor
            for key in descriptor.data_keys:
                data_path = "%s/data/%s" % (descriptor_path, key)
                # check the case when fields kwd is used
                if fields is not None:
                    if key not in fields:
                        assert data_path not in f
                        continue
                # make sure that the data path is in the file
                assert data_path in f
                # make sure the data is equivalent to what comes out of the
                # databroker
                hdf_data = np.asarray(f[data_path])
                broker_data = np.asarray(table[key])
                if isinstance(hdf_data[0], np.bytes_):
                    hdf_data = np.array(hdf_data).astype('str')
                if len(hdf_data.shape) == 2:
                    if isinstance(hdf_data[0,0], np.bytes_):
                        hdf_data = np.array(hdf_data).astype('str')
                np.testing.assert_array_equal(hdf_data, broker_data)
                # make sure the data is sorted in chronological order
                timestamps_path = "%s/timestamps/%s" % (descriptor_path, key)
                timestamps = np.asarray(f[timestamps_path])
                assert all(np.diff(timestamps) > 0)


def test_hdf5_export_single(db_all):
    """
    Test the hdf5 export with a single header and
    verify the output is correct
    """
    mds = db_all.mds
    temperature_ramp.run(mds)
    #db = Broker(mds, fs=None)
    hdr = db_all[-1]
    fname = tempfile.NamedTemporaryFile()
    hdf5.export(hdr, fname.name, db=db_all)
    shallow_header_verify(fname.name, hdr, db=db_all)


def test_hdf5_export_single_no_uid(db_all):
    """
    Test the hdf5 export with a single header and
    verify the output is correct. No uid is used.
    """
    mds = db_all.mds
    temperature_ramp.run(mds)
    hdr = db_all[-1]
    fname = tempfile.NamedTemporaryFile()
    hdf5.export(hdr, fname.name, use_uid=False, db=db_all)
    shallow_header_verify(fname.name, hdr, use_uid=False, db=db_all)


def test_hdf5_export_single_stream_name(db_all):
    """
    Test the hdf5 export with a single header and
    verify the output is correct. No uid is used.
    """
    mds = db_all.mds
    temperature_ramp.run(mds)
    hdr = db_all[-1]
    fname = tempfile.NamedTemporaryFile()
    hdf5.export(hdr, fname.name, stream_name='primary', db=db_all)
    shallow_header_verify(fname.name, hdr, stream_name='primary', db=db_all)


def test_hdf5_export_with_fields_single(db_all):
    """
    Test the hdf5 export with a single header and
    verify the output is correct; fields kwd is used.
    """
    mds = db_all.mds
    temperature_ramp.run(mds)
    hdr = db_all[-1]
    fname = tempfile.NamedTemporaryFile()
    hdf5.export(hdr, fname.name, fields=['point_dev'], db=db_all)
    shallow_header_verify(fname.name, hdr, fields=['point_dev'], db=db_all)


def test_filter_fields(db_all):
    mds = db_all.mds
    temperature_ramp.run(mds)
    hdr = db_all[-1]
    unwanted_fields = ['point_det']
    out = hdf5.filter_fields(hdr, unwanted_fields)
    #original list is ('point_det', 'boolean_det', 'ccd_det_info', 'Tsam'),
    # only ('boolean_det', 'ccd_det_info', 'Tsam') left after filtering out
    assert len(out)==3


def test_hdf5_export_list(db_all):
    """
    Test the hdf5 export with a list of headers and
    verify the output is correct
    """
    mds = db_all.mds
    temperature_ramp.run(mds)
    temperature_ramp.run(mds)
    hdrs = db_all[-2:]
    fname = tempfile.NamedTemporaryFile()
    # test exporting a list of headers
    hdf5.export(hdrs, fname.name, db=db_all)
    for hdr in hdrs:
        shallow_header_verify(fname.name, hdr, db=db_all)
