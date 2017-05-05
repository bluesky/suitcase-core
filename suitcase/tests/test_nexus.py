from metadatastore.examples.sample_data import temperature_ramp
from databroker import Broker
from suitcase import nexus
import tempfile
import h5py
import numpy as np
import pytest


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


def test_nexus_export_single(db_all):
    """
    Test the NeXus HDF5 export with a single header and
    verify the output is correct
    """
    mds = db_all.mds
    temperature_ramp.run(mds)
    hdr = db_all[-1]
    fname = tempfile.NamedTemporaryFile()
    nexus.export(hdr, fname.name, db=db_all)
    shallow_header_verify(fname.name, hdr, db_all)
    validate_basic_NeXus_structure(fname.name)


def test_nexus_export_single_no_uid(db_all):
    """
    Test the NeXus HDF5 export with a single header and
    verify the output is correct. No uid is used.
    """
    mds = db_all.mds
    temperature_ramp.run(mds)
    hdr = db_all[-1]
    fname = tempfile.NamedTemporaryFile()
    nexus.export(hdr, fname.name, use_uid=False, db=db_all)
    shallow_header_verify(fname.name, hdr, db_all, use_uid=False)
    validate_basic_NeXus_structure(fname.name)


def test_nexus_export_single_stream_name(db_all):
    """
    Test the NeXus HDF5 export with a single header and
    verify the output is correct. No uid is used.
    """
    mds = db_all.mds
    temperature_ramp.run(mds)
    hdr = db_all[-1]
    fname = tempfile.NamedTemporaryFile()
    nexus.export(hdr, fname.name, stream_name='primary', db=db_all)
    shallow_header_verify(fname.name, hdr, db_all, stream_name='primary')
    validate_basic_NeXus_structure(fname.name)


def test_nexus_export_with_fields_single(db_all):
    """
    Test the NeXus HDF5 export with a single header and
    verify the output is correct; fields kwd is used.
    """
    mds = db_all.mds
    temperature_ramp.run(mds)
    hdr = db_all[-1]
    fname = tempfile.NamedTemporaryFile()
    nexus.export(hdr, fname.name, fields=['point_dev'], db=db_all)
    shallow_header_verify(fname.name, hdr, db_all, fields=['point_dev'])
    validate_basic_NeXus_structure(fname.name)


def test_filter_fields(db_all):
    mds = db_all.mds
    temperature_ramp.run(mds)
    hdr = db_all[-1]
    unwanted_fields = ['point_det']
    out = nexus.filter_fields(hdr, unwanted_fields)
    #original list is ('point_det', 'boolean_det', 'ccd_det_info', 'Tsam'),
    # only ('boolean_det', 'ccd_det_info', 'Tsam') left after filtering out
    assert len(out)==3


def test_nexus_export_list(db_all):
    """
    Test the NeXus HDF5 export with a list of headers and
    verify the output is correct
    """
    mds = db_all.mds
    temperature_ramp.run(mds)
    temperature_ramp.run(mds)
    hdrs = db_all[-2:]
    fname = tempfile.NamedTemporaryFile()
    # test exporting a list of headers
    nexus.export(hdrs, fname.name, db=db_all)
    for hdr in hdrs:
        shallow_header_verify(fname.name, hdr, db_all)
        validate_basic_NeXus_structure(fname.name)


def test_nexus_runtime_error(db_all):
    mds = db_all.mds
    temperature_ramp.run(mds)
    hdr = db_all[-1]
    fname = tempfile.NamedTemporaryFile()
    if hasattr(hdr, 'db'):
        nexus.export(hdr, fname.name, db=None)
    else:
        with pytest.raises(RuntimeError):
            nexus.export(hdr, fname.name, db=None)
