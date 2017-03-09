from metadatastore.examples.sample_data import temperature_ramp
from databroker import Broker
from suitcase import nexus
import tempfile
import h5py
import numpy as np
import pytest


def shallow_header_verify(hdf_path, header, mds, fields=None, stream_name=None, use_uid=True):
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
            table_load = mds.get_events_table(descriptor)
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


def test_hdf5_export_single(mds_all):
    """
    Test the hdf5 export with a single header and
    verify the output is correct
    """
    mds = mds_all
    temperature_ramp.run(mds)
    db = Broker(mds, fs=None)
    hdr = db[-1]
    fname = tempfile.NamedTemporaryFile()
    nexus.export(hdr, fname.name, mds)
    shallow_header_verify(fname.name, hdr, mds)


def test_hdf5_export_single_no_uid(mds_all):
    """
    Test the hdf5 export with a single header and
    verify the output is correct. No uid is used.
    """
    mds = mds_all
    temperature_ramp.run(mds)
    db = Broker(mds, fs=None)
    hdr = db[-1]
    fname = tempfile.NamedTemporaryFile()
    nexus.export(hdr, fname.name, mds, use_uid=False)
    shallow_header_verify(fname.name, hdr, mds, use_uid=False)


def test_hdf5_export_single_stream_name(mds_all):
    """
    Test the hdf5 export with a single header and
    verify the output is correct. No uid is used.
    """
    mds = mds_all
    temperature_ramp.run(mds)
    db = Broker(mds, fs=None)
    hdr = db[-1]
    fname = tempfile.NamedTemporaryFile()
    nexus.export(hdr, fname.name, mds, stream_name='primary')
    shallow_header_verify(fname.name, hdr, mds, stream_name='primary')


def test_hdf5_export_with_fields_single(mds_all):
    """
    Test the hdf5 export with a single header and
    verify the output is correct; fields kwd is used.
    """
    mds = mds_all
    temperature_ramp.run(mds)
    db = Broker(mds, fs=None)
    hdr = db[-1]
    fname = tempfile.NamedTemporaryFile()
    nexus.export(hdr, fname.name, mds, fields=['point_dev'])
    shallow_header_verify(fname.name, hdr, mds, fields=['point_dev'])


def test_filter_fields(mds_all):
    mds = mds_all
    temperature_ramp.run(mds)
    db = Broker(mds, fs=None)
    hdr = db[-1]
    unwanted_fields = ['point_det']
    out = nexus.filter_fields(hdr, unwanted_fields)
    #original list is ('point_det', 'boolean_det', 'ccd_det_info', 'Tsam'),
    # only ('boolean_det', 'ccd_det_info', 'Tsam') left after filtering out
    assert len(out)==3


def test_hdf5_export_list(mds_all):
    """
    Test the hdf5 export with a list of headers and
    verify the output is correct
    """
    mds = mds_all
    temperature_ramp.run(mds)
    temperature_ramp.run(mds)
    db = Broker(mds, fs=None)
    hdrs = db[-2:]
    fname = tempfile.NamedTemporaryFile()
    # test exporting a list of headers
    nexus.export(hdrs, fname.name, mds)
    for hdr in hdrs:
        shallow_header_verify(fname.name, hdr, mds)
