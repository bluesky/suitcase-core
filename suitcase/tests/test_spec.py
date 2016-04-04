from __future__ import absolute_import, print_function, division
from suitcase.spec import Specfile, spec_to_document
import pytest
import os
from metadatastore.commands import (insert_run_start, insert_descriptor,
                                    insert_event, insert_run_stop)
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

