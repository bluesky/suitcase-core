from __future__ import absolute_import, print_function, division
from suitcase.spec import Specfile, spec_to_document
import pytest
import os
from metadatastore.commands import (insert_run_start, insert_descriptor,
                                    insert_event, insert_run_stop,
                                    get_events_generator)
from metadatastore.test.utils import mds_setup, mds_teardown

from databroker import db
from databroker.databroker import Header


def setup_function(function):
    mds_setup()


def teardown_function(function):
    mds_teardown()


@pytest.fixture(scope='module')
def spec_data():
    path = os.path.join(os.path.dirname(__file__), 'data', '20160219.spec')
    return Specfile(path)


def test_spec_parsing(spec_data):
    assert len(spec_data) == 34


def test_spec_to_document(spec_data):
    map = {
        'start': insert_run_start,
        'stop': insert_run_stop,
        'descriptor': insert_descriptor,
        'event': insert_event
    }
    start_uids = list()
    for document_name, document in spec_to_document(spec_data):
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

    for hdr, specscan in zip(hdrs, spec_data):
        for descriptor in hdr.descriptors:
            ev = list(get_events_generator(descriptor))
            if descriptor.name == 'baseline':
                # we better only have one baseline event
                assert len(ev) == 1
            else:
                assert len(specscan.scan_data) == len(ev)

def test_equality():
    path = os.path.join(os.path.dirname(__file__), 'data', '20160219.spec')
    sf1 = Specfile(path)
    sf2 = Specfile(path)
    assert sf1 == sf2
    for s1, s2, in zip(sf1, sf2):
        assert s1 == s2

def test_lt(spec_data):
    for s1, s2 in zip(spec_data, spec_data[1:]):
        assert s1 < s2
