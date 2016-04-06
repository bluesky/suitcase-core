from __future__ import absolute_import, print_function, division
from suitcase.spec import Specfile, spec_to_document, DocumentToSpec
import pytest
import os
from metadatastore.commands import (insert_run_start, insert_descriptor,
                                    insert_event, insert_run_stop,
                                    get_events_generator)
from metadatastore.test.utils import mds_setup, mds_teardown

from databroker import db
from databroker.databroker import Header
import tempfile


def setup_function(function):
    mds_setup()


def teardown_function(function):
    mds_teardown()


@pytest.fixture(scope='module')
def spec_filename():
    return os.path.join(os.path.dirname(__file__), 'data', '20160219.spec')


def test_spec_parsing(spec_filename):
    sf = Specfile(spec_filename)
    assert len(sf) == 34


def test_spec_to_document(spec_filename):
    sf = Specfile(spec_filename)
    map = {
        'start': insert_run_start,
        'stop': insert_run_stop,
        'descriptor': insert_descriptor,
        'event': insert_event
    }
    start_uids = list()
    for document_name, document in spec_to_document(sf):
        # insert the documents
        if document_name == 'start':
            document['beamline_id'] = 'test'
            start_uids.append(document['uid'])
        map[document_name](**document)

    # make sure we are not trying to add duplicates
    assert len(start_uids) == len(set(start_uids))

    # smoketest the retrieval
    hdrs = []
    for uid in start_uids:
        hdr = db[uid]
        # make sure we only get one back
        assert isinstance(hdr, Header)
        hdrs.append(hdr)

    # make sure we are not getting duplicates back out
    hdr_uids = [hdr.start.uid for hdr in hdrs]
    assert len(hdr_uids) == len(set(hdr_uids))

    for hdr, specscan in zip(hdrs, sf):
        for descriptor in hdr.descriptors:
            ev = list(get_events_generator(descriptor))
            if descriptor.name == 'baseline':
                # we better only have one baseline event
                assert len(ev) == 1
            else:
                assert len(specscan.scan_data) == len(ev)

def test_equality(spec_filename):
    sf1 = Specfile(spec_filename)
    sf2 = Specfile(spec_filename)
    assert sf1 == sf2
    for s1, s2, in zip(sf1, sf2):
        assert s1 == s2

def test_lt(spec_filename):
    sf = Specfile(spec_filename)
    for s1, s2 in zip(sf, sf[1:]):
        assert s1 < s2


def _round_trip(specfile_object, new_specfile_name=None):
    if new_specfile_name is None:
        new_specfile_name = tempfile.NamedTemporaryFile().name

    document_stream = spec_to_document(specfile_object)
    fname = tempfile.NamedTemporaryFile().name
    cb = DocumentToSpec(fname)
    for doc_name, doc in document_stream:
        # RunEngine.subscribe does the translation of 'start' <->
        # event_model.DocumentNames.start under the hood. Since we do not have
        # this magic here, we have to do it by hand
        cb(doc_name.name, doc)

    sf1 = Specfile(fname)

    return sf1



def test_round_trip_from_specfile(spec_filename):
    sf = Specfile(spec_filename)
    sf1 = _round_trip(sf)

    # this will probably fail because we cannot convert *all* spec scan types.
    with pytest.raises(AssertionError):
        assert len(sf) == len(sf1)

    # round trip again
    sf2 = _round_trip(sf1)
    assert len(sf1) == len(sf2)
    assert len(sf2) > 0


def test_round_trip_from_run_engine():
    # generate a new specfile
    from bluesky.tests.utils import setup_test_run_engine
    from bluesky.examples import motor, det
    from bluesky.plans import DeltaScanPlan, AbsScanPlan, Count, Tweak
    RE = setup_test_run_engine()
    RE.ignore_callback_exceptions = False
    fname = tempfile.NamedTemporaryFile().name
    cb = DocumentToSpec(fname)
    dscan = DeltaScanPlan([det], motor, -1, 1, 10)
    RE(dscan, {'all': cb})
    ascan = AbsScanPlan([det], motor, -1, 1, 10)
    RE(ascan, {'all': cb})


    sf = Specfile(fname)
    sf1 = _round_trip(sf)

    assert len(sf) == len(sf1)
