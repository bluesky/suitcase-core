from __future__ import absolute_import, print_function, division

import itertools
import logging
import os
import tempfile

import event_model
import pytest
from metadatastore.commands import (insert_run_start, insert_descriptor,
                                    insert_event, insert_run_stop,
                                    get_events_generator)
from metadatastore.test.utils import mds_setup, mds_teardown

from databroker import db
from databroker.core import Header
from suitcase.spec import (Specfile, spec_to_document, DocumentToSpec,
                           Specscan, specscan_to_document_stream,
                           insert_into_broker, logger)
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
logger.setLevel(logging.DEBUG)
logger.addHandler(stream_handler)

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


def test_spec_attrs_smoke(spec_filename):
    sf = Specfile(spec_filename)
    # smoketest Specfile.__str__
    str(sf)
    # smoketest Specfile.__getitem__
    scan = sf[1]
    # smoketest Specscan.__repr__
    repr(scan)
    # smoketest Specscan.__len__
    len(scan)
    # smoketest Specscan.__str__
    str(scan)



@pytest.mark.xfail(reason='Testing `spec_to_document` with bad input')
def test_spec_to_document_bad_input():
    list(spec_to_document(2))


@pytest.mark.parametrize(
    "sf, scan_ids",
    itertools.product(
        [spec_filename(),
         Specfile(spec_filename()),
         Specfile(spec_filename())[1]],
        [1, [1, 2], None]))
def test_spec_to_document(sf, scan_ids):
    map = {
        'start': insert_run_start,
        'stop': insert_run_stop,
        'descriptor': insert_descriptor,
        'event': insert_event
    }
    start_uids = list()
    for document_name, document in spec_to_document(sf,
                                                    scan_ids=scan_ids,
                                                    validate=True):
        document = dict(document)
        del document['_name']
        if not isinstance(document_name, str):
            document_name = document_name.name
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
    if isinstance(sf, Specscan):
        sf = [sf]
    if isinstance(sf, str):
        sf = Specfile(sf)
    for hdr, specscan in zip(hdrs, sf):
        for descriptor in hdr.descriptors:
            ev = list(get_events_generator(descriptor))
            if descriptor.get('name') == 'baseline':
                # we better only have one baseline event
                assert len(ev) == 1
            else:
                assert len(specscan.scan_data) == len(ev)

def test_equality(spec_filename):
    sf1 = Specfile(spec_filename)
    sf2 = Specfile(spec_filename)
    assert sf1 == sf2

    assert sf1 != 'cat'
    assert sf1[-1] != 'cat'

def test_lt(spec_filename):
    sf = Specfile(spec_filename)
    for s1, s2 in zip(sf, sf[1:]):
        assert s1 < s2


def _round_trip(specfile_object, new_specfile_name=None):
    if new_specfile_name is None:
        new_specfile_name = tempfile.NamedTemporaryFile().name

    document_stream = spec_to_document(specfile_object)
    cb = DocumentToSpec(new_specfile_name)
    for doc_name, doc in document_stream:
        # RunEngine.subscribe does the translation of 'start' <->
        # event_model.DocumentNames.start under the hood. Since we do not have
        # this magic here, we have to do it by hand
        cb(doc_name.name, doc)

    sf1 = Specfile(new_specfile_name)

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
    try:
        import bluesky
    except ImportError as ie:
        raise pytest.skip('ImportError: {0}'.format(ie))
    # generate a new specfile
    from bluesky.tests.utils import setup_test_run_engine
    from bluesky.examples import motor, det, motor1
    from bluesky.global_state import gs
    from bluesky.spec_api import dscan, ascan, ct, a2scan
    RE = setup_test_run_engine()
    RE.ignore_callback_exceptions = False
    fname = tempfile.NamedTemporaryFile().name
    cb = DocumentToSpec(fname)
    RE.subscribe('all', cb)
    gs.DETS = [det]
    RE(dscan(motor, -1, 1, 10))
    RE(ascan(motor, -1, 1, 10))
    # add count to hit some lines in
    #   suitcase.spec:_get_motor_name
    #   suitcase.spec:_get_motor_position
    #   suitcase.spec:_get_plan_type
    RE(ct())

    RE(a2scan(motor, -1, 1, motor1, -1, 1, 10))

    sf = Specfile(fname)
    sf1 = _round_trip(sf)

    # a2scan is not round trippable
    num_unconvertable_scans = 1

    assert len(sf) == (len(sf1) + num_unconvertable_scans)


def _insert_helper(specscan):
    doc_gen = specscan_to_document_stream(specscan, validate=True)

    for doc_name, doc in doc_gen:
        if doc_name == event_model.DocumentNames.start:
            print('inserting {}'.format(doc_name))
            insert_run_start(**doc)
        elif doc_name == event_model.DocumentNames.descriptor:
            print('inserting {}'.format(doc_name))
            insert_descriptor(**doc)
        elif doc_name == event_model.DocumentNames.event:
            print('inserting {}'.format(doc_name))
            # can't send in the _name attribute
            ev_doc = dict(doc)
            del ev_doc['_name']
            insert_event(**ev_doc)
        elif doc_name == event_model.DocumentNames.stop:
            print('inserting {}'.format(doc_name))
            insert_run_stop(**doc)


@pytest.mark.parametrize(
    'specdata',
    [[Specfile(spec_filename())[1]],  # Send in one specscan
      Specfile(spec_filename())])  # Send in a whole specfile
def test_insert_document_stream(specdata):
    from suitcase.spec import _find_map
    import metadatastore.commands as mdsc

    for scan in specdata:
        # make sure that none of these documents exists in metadatastore
        for doc_name, doc in specscan_to_document_stream(scan, validate=False):
            assert not list(getattr(mdsc, _find_map[doc_name])(uid=doc.uid))

        # now insert these documents into the databroker
        insert_into_broker(scan)

        # then make sure that all of them made it in
        for checked, unchecked in zip(
                specscan_to_document_stream(scan, check_in_broker=True),
                specscan_to_document_stream(scan, check_in_broker=False)):
            assert checked[1].uid != unchecked[1].uid

#
# def test_double_insert_document_stream(spec_filename):
#     sf = Specfile(spec_filename)
#     scan = next(iter(sf))
#     insert_into_broker(scan)
#     insert_into_broker(scan)
