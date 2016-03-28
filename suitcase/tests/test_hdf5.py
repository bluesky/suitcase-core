
from metadatastore.test.utils import mds_setup, mds_teardown
from metadatastore.examples.sample_data import (multisource_event,
                                                temperature_ramp)
from databroker import db
from suitcase import hdf5
import tempfile


def setup_function(function):
    mds_setup()


def teardown_function(function):
    mds_teardown()


def test_hdf5_export_single():
    temperature_ramp.run()
    hdr = db[-1]
    fname = tempfile.NamedTemporaryFile()
    hdf5.export(hdr, fname.name)


def test_hdf5_export_list():
    temperature_ramp.run()
    temperature_ramp.run()
    hdrs = db[-2:]
    fname = tempfile.NamedTemporaryFile()
    # test exporting a list of headers
    hdf5.export(hdrs, fname.name)
