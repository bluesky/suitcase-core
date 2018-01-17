from suitcase import hdf5
import tempfile
import h5py
import sys
import numpy as np
import pytest

if sys.version_info >= (3, 5):
    from bluesky.plans import count, scan
    from bluesky.examples import motor, det


def shallow_header_verify(hdf_path, header, db, fields=None,
                          stream_name=None, use_uid=True):
    with h5py.File(hdf_path) as f:
        # make sure that the header is actually in the file that we think it is
        # supposed to be in
        if use_uid:
            sub_path = header.start.uid
        else:
            sub_path = 'data_' + str(header.start.scan_id)
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
                    hdf_data = np.array(hdf_data).astype('<U3')
                if len(hdf_data.shape) == 2:
                    if isinstance(hdf_data[0,0], np.bytes_):
                        hdf_data = np.array(hdf_data).astype('<U3')
                np.testing.assert_array_equal(hdf_data, broker_data)
                # make sure the data is sorted in chronological order
                timestamps_path = "%s/timestamps/%s" % (descriptor_path, key)
                timestamps = np.asarray(f[timestamps_path])
                assert all(np.diff(timestamps) > 0)


@pytest.mark.skipif(sys.version_info < (3,5),
                    reason="bluesky related tests need python 3.5, 3.6")
def test_hdf5_export_single(db_all, RE, hw):
    """
    Test the hdf5 export with a single header and
    verify the output is correct
    """
    RE.subscribe(db_all.insert)
    hw.motor.delay = 0.1
    RE(scan([hw.det], hw.motor, -1, 1, 10), owner="Tom")
    hdr = db_all[-1]
    fname = tempfile.NamedTemporaryFile()
    hdf5.export(hdr, fname.name, db=db_all)
    shallow_header_verify(fname.name, hdr, db=db_all)


@pytest.mark.skipif(sys.version_info < (3,5),
                    reason="bluesky related tests need python 3.5, 3.6")
def test_hdf5_export_single_no_uid(db_all, RE, hw):
    """
    Test the hdf5 export with a single header and
    verify the output is correct. No uid is used.
    """
    RE.subscribe(db_all.insert)
    hw.motor.delay = 0.1
    RE(scan([hw.det], hw.motor, -1, 1, 10), owner="Tom")
    hdr = db_all[-1]
    fname = tempfile.NamedTemporaryFile()
    hdf5.export(hdr, fname.name, use_uid=False, db=db_all)
    shallow_header_verify(fname.name, hdr, db_all, use_uid=False)


@pytest.mark.skipif(sys.version_info < (3,5),
                    reason="bluesky related tests need python 3.5, 3.6")
def test_hdf5_export_single_stream_name(db_all, RE, hw):
    """
    Test the hdf5 export with a single header and
    verify the output is correct. No uid is used.
    """
    RE.subscribe(db_all.insert)
    hw.motor.delay = 0.1
    RE(scan([hw.det], hw.motor, -1, 1, 10), owner="Tom")
    hdr = db_all[-1]
    fname = tempfile.NamedTemporaryFile()
    hdf5.export(hdr, fname.name, stream_name='primary', db=db_all)
    shallow_header_verify(fname.name, hdr, db_all, stream_name='primary')


@pytest.mark.skipif(sys.version_info < (3,5),
                    reason="bluesky related tests need python 3.5, 3.6")
def test_hdf5_export_with_fields_single(db_all, RE, hw):
    """
    Test the hdf5 export with a single header and
    verify the output is correct; fields kwd is used.
    """
    RE.subscribe(db_all.insert)
    hw.motor.delay = 0.1
    RE(scan([hw.det], hw.motor, -1, 1, 10), owner="Tom")
    hdr = db_all[-1]
    fname = tempfile.NamedTemporaryFile()
    hdf5.export(hdr, fname.name, fields=['point_dev'])#, db=db_all)
    shallow_header_verify(fname.name, hdr, db_all, fields=['point_dev'])


@pytest.mark.skipif(sys.version_info < (3,5),
                    reason="bluesky related tests need python 3.5, 3.6")
def test_filter_fields(db_all, RE, hw):
    RE.subscribe(db_all.insert)
    hw.motor.delay = 0.1
    RE(scan([hw.det], hw.motor, -1, 1, 10), owner="Tom")
    hdr = db_all[-1]
    unwanted_fields = ['det']
    out = hdf5.filter_fields(hdr, unwanted_fields)
    assert len(out)==2 # still two fields left ['motor', 'motor_setpoint']
    unwanted_fields = ['no-exist-name']
    out = hdf5.filter_fields(hdr, unwanted_fields)
    assert len(out)==3  # three fields in total


@pytest.mark.skipif(sys.version_info < (3,5),
                    reason="bluesky related tests need python 3.5, 3.6")
def test_hdf5_export_list(db_all, RE):
    """
    Test the hdf5 export with a list of headers and
    verify the output is correct
    """
    RE.subscribe(db_all.insert)
    RE(count([det], 5, delay = 1), owner="Tom")
    RE(count([det], 6, delay = 0.1), owner="Ken")
    hdrs = db_all[-2:]
    fname = tempfile.NamedTemporaryFile()
    # test exporting a list of headers
    hdf5.export(hdrs, fname.name, db=db_all)
    for hdr in hdrs:
        shallow_header_verify(fname.name, hdr, db=db_all)


@pytest.mark.skipif(sys.version_info < (3,5),
                    reason="bluesky related tests need python 3.5, 3.6")
def test_hdf5_runtime_error(db_all, RE):
    RE.subscribe(db_all.insert)
    RE(count([det], 5, delay = 1), owner="Tom")
    hdr = db_all[-1]
    fname = tempfile.NamedTemporaryFile()
    if hasattr(hdr, 'db'):
        hdf5.export(hdr, fname.name, db=None)
    else:
        with pytest.raises(RuntimeError):
            hdf5.export(hdr, fname.name, db=None)
