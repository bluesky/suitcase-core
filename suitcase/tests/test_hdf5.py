
from metadatastore.test.utils import mds_setup, mds_teardown
from metadatastore.examples.sample_data import (multisource_event,
                                                temperature_ramp)
from databroker import db, get_table
from suitcase import hdf5
import tempfile
import h5py
import numpy as np

def setup_function(function):
    mds_setup()


def teardown_function(function):
    mds_teardown()


def shallow_header_verify(hdf_path, header):
    table = get_table(header)
    with h5py.File(hdf_path) as f:
        assert header.start.uid in f
        # make sure the descriptors are all in the hdf output file
        for descriptor in header.descriptors:
            descriptor_path = '%s/%s' % (header.start.uid, descriptor.uid)
            assert descriptor_path in f
            # make sure all keys are in each descriptor
            for key in descriptor.data_keys:
                data_path = "%s/data/%s" % (descriptor_path, key)
                # make sure that the data path is in the file
                assert data_path in f
                # make sure the data is equivalent to what comes out of the
                # databroker
                hdf_data = np.asarray(f[data_path])
                broker_data = table[key].dropna().values
                assert all(hdf_data == broker_data)
                # make sure the data is sorted in chronological order
                timestamps_path = "%s/timestamps/%s" % (descriptor_path, key)
                timestamps = np.asarray(f[timestamps_path])
                assert all(np.diff(timestamps) > 0)


def test_hdf5_export_single():
    """
    Test the hdf5 export with a single header and
    verify the output is correct
    """
    temperature_ramp.run()
    hdr = db[-1]
    fname = tempfile.NamedTemporaryFile()
    hdf5.export(hdr, fname.name)
    shallow_header_verify(fname.name, hdr)


def test_hdf5_export_list():
    """
    Test the hdf5 export with a list of headers and
    verify the output is correct
    """
    temperature_ramp.run()
    temperature_ramp.run()
    hdrs = db[-2:]
    fname = tempfile.NamedTemporaryFile()
    # test exporting a list of headers
    hdf5.export(hdrs, fname.name)
    for hdr in hdrs:
        shallow_header_verify(fname.name, hdr)
