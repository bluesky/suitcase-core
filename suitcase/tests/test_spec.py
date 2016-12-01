from __future__ import absolute_import, print_function, division

import itertools
import logging
import os
import tempfile

import event_model
import pytest
from databroker.broker import Broker
from databroker.core import Header
from suitcase import spec

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
spec.logger.setLevel(logging.DEBUG)
spec.logger.addHandler(stream_handler)


@pytest.fixture(scope='module')
def spec_filename():
    return os.path.join(os.path.dirname(__file__), 'data', 'sample-spec-file')


@pytest.fixture(scope='module')
def specfile_no_scandata():
    return os.path.join(os.path.dirname(__file__), 'data',
                        'specfile_no_scandata')


@pytest.fixture(scope='module')
def specfile_no_header():
    return os.path.join(os.path.dirname(__file__), 'data',
                        'no-file-header')


@pytest.fixture(scope='module')
def specfile_multiple_headers():
    return os.path.join(os.path.dirname(__file__), 'data',
                        'multiple-file-headers')


@pytest.mark.xfail(reason='Making sure that multiple file headers in one '
                          'file raises a NotImplementedError')
def test_multiple_headers(specfile_multiple_headers):
    spec.Specfile(specfile_multiple_headers)


@pytest.mark.xfail(reason='Making sure that no file header in a specfile '
                          'raises a NotImplementedError')
def test_bad_header(specfile_no_header):
    spec.Specfile(specfile_no_header)


def test_spec_parsing(spec_filename):
    sf = spec.Specfile(spec_filename)
    assert len(sf) == 2


@pytest.mark.parametrize('sf', [spec.Specfile(spec_filename()),
                                      spec.Specfile(specfile_no_scandata())])
def test_spec_attrs_smoke(sf):
    # smoketest Specfile.__str__
    str(sf)
    # smoketest Specfile.__getitem__
    scans = [sf[scan.scan_id] for scan in sf]
    scan = scans[0]
    # smoketest Specscan.__repr__
    repr(scan)
    # smoketest Specscan.__len__
    len(scan)
    # smoketest Specscan.__str__
    str(scan)


@pytest.mark.xfail(reason='Testing `spec_to_document` with bad input')
def test_spec_to_document_bad_input():
    list(spec.spec_to_document(2))


@pytest.mark.parametrize(
    "sf, scan_ids",
    itertools.product(
        [spec_filename(),
         spec.Specfile(spec_filename()),
         spec.Specfile(spec_filename())[1]],
        [1, [1, 2], None]))
def test_spec_to_document(sf, mds_all, scan_ids):
    map = {
        'start': mds_all.insert_run_start,
        'stop': mds_all.insert_run_stop,
        'descriptor': mds_all.insert_descriptor,
        'event': mds_all.insert_event
    }
    start_uids = list()

    db = Broker(mds_all, fs=None)

    for document_name, document in spec.spec_to_document(
            sf, mds_all, scan_ids=scan_ids, validate=True):
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
    if isinstance(sf, spec.Specscan):
        sf = [sf]
    if isinstance(sf, str):
        sf = spec.Specfile(sf)
    for hdr, specscan in zip(hdrs, sf):
        for descriptor in hdr.descriptors:
            ev = list(mds_all.get_events_generator(descriptor))
            if descriptor.get('name') == 'baseline':
                # we better only have one baseline event
                assert len(ev) == 1
            else:
                assert len(specscan.scan_data) == len(ev)


def test_equality(spec_filename):
    sf1 = spec.Specfile(spec_filename)
    sf2 = spec.Specfile(spec_filename)
    assert sf1 == sf2

    assert sf1 != 'cat'
    assert sf1[-1] != 'cat'


def test_lt(spec_filename):
    sf = spec.Specfile(spec_filename)
    for s1, s2 in zip(sf, sf[1:]):
        assert s1 < s2


def _round_trip(specfile_object, mds_all, new_specfile_name=None):
    if new_specfile_name is None:
        new_specfile_name = tempfile.NamedTemporaryFile().name

    document_stream = spec.spec_to_document(specfile_object, mds_all)
    cb = spec.DocumentToSpec(new_specfile_name)
    for doc_name, doc in document_stream:
        # RunEngine.subscribe does the translation of 'start' <->
        # event_model.DocumentNames.start under the hood. Since we do not have
        # this magic here, we have to do it by hand
        cb(doc_name.name, doc)

    sf1 = spec.Specfile(new_specfile_name)

    return sf1


def test_round_trip_from_specfile(spec_filename, mds_all):
    sf = spec.Specfile(spec_filename)
    sf1 = _round_trip(sf, mds_all)

    # this will probably fail because we cannot convert *all* spec scan types.
    with pytest.raises(AssertionError):
        assert len(sf) == len(sf1)

    # round trip again
    sf2 = _round_trip(sf1, mds_all)
    assert len(sf1) == len(sf2)
    assert len(sf2) > 0


def test_round_trip_from_run_engine(mds_all):
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
    fname = tempfile.NamedTemporaryFile().name
    cb = spec.DocumentToSpec(fname)
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

    sf = spec.Specfile(fname)
    sf1 = _round_trip(sf, mds_all)

    # a2scan is not round trippable
    num_unconvertable_scans = 1

    assert len(sf) == (len(sf1) + num_unconvertable_scans)


def test_insert_specscan(spec_filename, mds_all):
    specfile = spec.Specfile(spec_filename)
    scan = next(iter(specfile))
    spec.insert_specscan_into_broker(scan, mds=mds_all)


def test_insert_specfile(spec_filename, mds_all):
    specfile = spec.Specfile(spec_filename)
    # Can only insert the spec scans whose type is in the _BLUESKY_PLAN_NAMES
    # list
    scans_expected_to_fail = [scan for scan in specfile if scan.scan_command
                              not in spec._BLUESKY_PLAN_NAMES]
    suceeded, failed = spec.insert_specfile_into_broker(specfile, mds=mds_all)
    assert len(scans_expected_to_fail) == len(failed)

    # smoketest the format output
    spec.summarize_insertion(suceeded, failed)


def test_double_insert_specscan(spec_filename, mds_all):
    specfile = spec.Specfile(spec_filename)
    scan = next(iter(specfile))
    uids = spec.insert_specscan_into_broker(scan, mds=mds_all)
    assert sum([uid[2] for uid in uids]) == len(uids)
    uids = spec.insert_specscan_into_broker(scan, mds=mds_all)
    assert sum([uid[2] for uid in uids]) == 0


@pytest.mark.xfail(reason='Testing `insert_into_broker` with bad input')
def test_bad_document_stream():
    spec.insert_specscan_into_broker('cat')
