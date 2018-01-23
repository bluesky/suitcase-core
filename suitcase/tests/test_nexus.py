from suitcase import nexus
import tempfile
import h5py
import sys
import numpy as np
import pytest

if sys.version_info >= (3, 5):
    from bluesky.plans import count, scan
    from bluesky.examples import motor, det


def shallow_header_verify(hdf_path, header, db, fields=None, stream_name=None, use_uid=True):
    with h5py.File(hdf_path) as f:
        # make sure that the header is actually in the file that we think it is
        # supposed to be in
        if use_uid:
            sub_path = header.start.uid
        else:
            sub_path = 'data_' + str(header.start.scan_id)
        safe_subpath = nexus.pick_NeXus_safe_name(sub_path)
        assert safe_subpath in f
        assert dict(header.start) == eval(f[safe_subpath].attrs[nexus.ATTRIBUTE_PREFIX + 'start'])
        assert dict(header.stop) == eval(f[safe_subpath].attrs[nexus.ATTRIBUTE_PREFIX + 'stop'])

        # make sure the descriptors are all in the hdf output file
        for descriptor in header.descriptors:
            if stream_name is not None:
                if stream_name != descriptor.name:
                    continue
            table_load = db.mds.get_events_table(descriptor)
            table = table_load[1]  # only get data
            if use_uid:
                descriptor_path = '%s/%s' % (safe_subpath, nexus.pick_NeXus_safe_name(descriptor.uid))
                assert descriptor_path in f
            else:
                descriptor_path = '%s/%s' % (safe_subpath, nexus.pick_NeXus_safe_name(descriptor.name))
                assert descriptor_path in f
            # make sure all keys are in each descriptor
            for key in descriptor.data_keys:
                safe_key = nexus.pick_NeXus_safe_name(key)
                data_path = "%s/%s" % (descriptor_path, safe_key)
                # check the case when fields kwd is used
                if fields is not None:
                    if safe_key not in fields:
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
                timestamps_path = "%s/%s_timestamps" % (descriptor_path, safe_key)
                timestamps = np.asarray(f[timestamps_path])
                assert all(np.diff(timestamps) > 0)


def validate_basic_NeXus_structure(hdf_path):
    with h5py.File(hdf_path) as f:

        # check for default path to plottable data
        if f.attrs.get('default') is not None:
            assert f.attrs.get('default') in f
            nexentry = f[f.attrs.get('default')]
            if nexentry.attrs.get('default') is not None:
                assert nexentry.attrs.get('default') in nexentry
                nxdata = nexentry[nexentry.attrs.get('default')]
                if nxdata.attrs.get('signal') is not None:
                    assert nxdata.attrs.get('signal') in nxdata
                    signal = nxdata[nxdata.attrs.get('signal')]

        # TODO: look for empty NXdata groups
        # TODO: check that NXdata/@signal points to existing dataset


@pytest.mark.skipif(sys.version_info < (3,5),
                    reason="bluesky related tests need python 3.5, 3.6")
def test_nexus_export_single(db_all, RE):
    """
    Test the NeXus HDF5 export with a single header and
    verify the output is correct
    """
    RE.subscribe(db_all.insert)
    RE(count([det], 5, delay = 0.1), owner="Tom")
    hdr = db_all[-1]
    fname = tempfile.NamedTemporaryFile()
    nexus.export(hdr, fname.name, db=db_all)
    shallow_header_verify(fname.name, hdr, db_all)
    validate_basic_NeXus_structure(fname.name)


@pytest.mark.skipif(sys.version_info < (3,5),
                    reason="bluesky related tests need python 3.5, 3.6")
def test_nexus_export_single_no_uid(db_all, RE):
    """
    Test the NeXus HDF5 export with a single header and
    verify the output is correct. No uid is used.
    """
    RE.subscribe(db_all.insert)
    RE(count([det], 5, delay = 0.1), owner="Tom")
    hdr = db_all[-1]
    fname = tempfile.NamedTemporaryFile()
    nexus.export(hdr, fname.name, use_uid=False, db=db_all)
    shallow_header_verify(fname.name, hdr, db_all, use_uid=False)
    validate_basic_NeXus_structure(fname.name)


@pytest.mark.skipif(sys.version_info < (3,5),
                    reason="bluesky related tests need python 3.5, 3.6")
def test_nexus_export_single_stream_name(db_all, RE):
    """
    Test the NeXus HDF5 export with a single header and
    verify the output is correct. No uid is used.
    """
    RE.subscribe(db_all.insert)
    RE(count([det], 5, delay = 0.1), owner="Tom")
    hdr = db_all[-1]
    fname = tempfile.NamedTemporaryFile()
    nexus.export(hdr, fname.name, stream_name='primary', db=db_all)
    shallow_header_verify(fname.name, hdr, db_all, stream_name='primary')
    validate_basic_NeXus_structure(fname.name)


@pytest.mark.skipif(sys.version_info < (3,5),
                    reason="bluesky related tests need python 3.5, 3.6")
def test_nexus_export_with_fields_single(db_all, RE):
    """
    Test the NeXus HDF5 export with a single header and
    verify the output is correct; fields kwd is used.
    """
    RE.subscribe(db_all.insert)
    RE(count([det], 5, delay = 0.1), owner="Tom")
    hdr = db_all[-1]
    fname = tempfile.NamedTemporaryFile()
    nexus.export(hdr, fname.name, fields=['point_dev'], db=db_all)
    shallow_header_verify(fname.name, hdr, db_all, fields=['point_dev'])
    validate_basic_NeXus_structure(fname.name)


@pytest.mark.skipif(sys.version_info < (3,5),
                    reason="bluesky related tests need python 3.5, 3.6")
def test_filter_fields(db_all, RE):
    RE.subscribe(db_all.insert)
    RE(count([det], 5, delay = 0.1), owner="Tom")
    hdr = db_all[-1]
    unwanted_fields = ['det']
    out = nexus.filter_fields(hdr, unwanted_fields)
    assert len(out)==0
    unwanted_fields = ['no-exist-name']
    out = nexus.filter_fields(hdr, unwanted_fields)
    assert len(out)==1


@pytest.mark.skipif(sys.version_info < (3,5),
                    reason="bluesky related tests need python 3.5, 3.6")
def test_nexus_export_list(db_all, RE):
    """
    Test the NeXus HDF5 export with a list of headers and
    verify the output is correct
    """
    RE.subscribe(db_all.insert)
    RE(count([det], 5, delay = 0.1), owner="Tom")
    RE(count([det], 10, delay = 0.1), sample="Cu")
    hdrs = db_all[-2:]
    fname = tempfile.NamedTemporaryFile()
    # test exporting a list of headers
    nexus.export(hdrs, fname.name, db=db_all)
    for hdr in hdrs:
        shallow_header_verify(fname.name, hdr, db_all)
        validate_basic_NeXus_structure(fname.name)


@pytest.mark.skipif(sys.version_info < (3,5),
                    reason="bluesky related tests need python 3.5, 3.6")
def test_nexus_runtime_error(db_all, RE):
    RE.subscribe(db_all.insert)
    RE(count([det], 5, delay = 0.1), owner="Tom")
    hdr = db_all[-1]
    fname = tempfile.NamedTemporaryFile()
    if hasattr(hdr, 'db'):
        nexus.export(hdr, fname.name, db=None)
    else:
        with pytest.raises(RuntimeError):
            nexus.export(hdr, fname.name, db=None)
