"""
`Reference <https://github.com/certified-spec/specPy/blob/master/doc/specformat.rst>`_
for the spec file format.
"""
import copy
import logging
import os
import uuid
import warnings
from collections import namedtuple, defaultdict
from datetime import datetime

import doct
import event_model
import jinja2
import jsonschema
import numpy as np
import pandas as pd
import six
from prettytable import PrettyTable

try:
    from functools import singledispatch
except ImportError:
    # LPy...
    from singledispatch import singledispatch

logger = logging.getLogger(__name__)

try:
    import metadatastore.commands as mdsc
except ImportError:
    logger.error("metadatastore not available. Some functionality is disabled")
    mdsc = None


# The way that SPEC time is formatted
SPEC_TIME_FORMAT = '%a %b %d %H:%M:%S %Y'
def from_spec_time(string_time):
    """Convert the spec time in line #D to a Python datetime object

    Parameters
    ----------
    string_time : str
        The SPEC string representation of time. e.g.: Fri Feb 19 14:01:35 2016

    Returns
    -------
    datetime.datetime object
    """
    return datetime.strptime(string_time, SPEC_TIME_FORMAT)

def to_spec_time(datetime_object):
    """Convert a datetime object into the SPEC line #D

    Parameters
    ----------
    datetime_object : datetime.datetime object

    Returns
    -------
    str
        The string representation of SPEC time: e.g., Fri Feb 19 14:01:35 2016
    """
    return datetime_object.strftime(SPEC_TIME_FORMAT)

# Dictionary that maps a spec metadata line to a specific lambda function
# to parse it. This only works for lines whose contents can be mapped to a
# single semantic meaning.  e.g., the "spec command" line
# (ascan start stop step exposure_time) does not map well on to this "single
# semantic meaning" splitter
spec_line_parser = {
    '#D': ('time_from_date', from_spec_time),
    '#E': ('time',
           lambda x: datetime.fromtimestamp(int(x))),
    '#F': ('filename', str),
    # The exposure time
    '#N': ('num_points', int),
    # The h, k, l coordinates
    '#Q': ('hkl', lambda x: [float(s) for s in x.split(' ')]),
    '#T': ('exposure_time', lambda x: float(x.split('  ')[0])),
}


def parse_spec_header(spec_header):
    """Parse the spec header!

    Parameters
    ----------
    spec_header : list
        List of the lines in the spec file header. This is the block of text
        in the spec file before the first scan. Note that the first scan
        starts with a line that begins with "#S"

    Returns
    -------
    parsed_header : dict
        The spec header parsed into a dictionary with much more useful names
    """
    # initialize the header dictionary that contains a mapping of more useful
    # names than #O, #o, etc..., along with python objects for each type of
    # metadata
    parsed_header = {
        "motor_human_names": [],
        "motor_spec_names": [],
        "detector_human_names": [],
        "detector_spec_names": [],
    }
    # map the motor/det lines to the dictionary of more friendly names
    spec_obj_map = {
        '#O': ('  ', parsed_header['motor_human_names']),
        '#o': (' ',  parsed_header['motor_spec_names']),
        '#J': ('  ', parsed_header['detector_human_names']),
        '#j': (' ',  parsed_header['detector_spec_names'])
    }
    for line in spec_header:
        if not line.startswith('#'):
            # this is not a line that contains information that I care about
            continue
        # split the line into the "line_type" which tells us the type of
        # information that this line contains and the "line_contents" which
        # tells us
        line_type, line_contents = line.split(' ', 1)
        if line_type[:2] in spec_obj_map:
            # these are lines whose semantic information spreads across
            # multiple lines
            sep, lst = spec_obj_map[line_type[:2]]
            lst.extend(line_contents.strip().split(sep))
        elif line_type == '#C':
            # this line looks like this: '#C fourc  User = asuvorov'
            # and it contains two pieces of information. Have to special case
            # it
            spec_mode, user = line_contents.split('  ')
            parsed_header['spec_mode'] = spec_mode
            parsed_header['user'] = user.split()[-1]
        elif line_type in spec_line_parser:
            # These lines are self contained and map to one piece of
            # information
            attr, func = spec_line_parser[line_type]
            parsed_header[attr] = func(line_contents)
        else:
            # I have no idea what to do with this line...
            warnings.warn("I am not sure how to parse %s" % line_type)
            parsed_header[line_type] = line_contents

    return parsed_header


def parse_spec_scan(raw_scan_data):
    """Parse the spec scan!

    Parameters
    ----------
    raw_scan_data : iterable
        Iterable of the lines in the spec scan. Should include the scan header
        and the scan data

    Returns
    -------
    md : dict
        The contents of the scan header parsed into a dictionary of python
        objects
    scan_data : pandas.DataFrame
        The scan data in a pandas data frame.  Column names come from the #L
        line
    """
    md = {}
    S_row = raw_scan_data[0].split()
    line_type = S_row.pop(0)  # this is the '#S' portion of this line
    md['scan_id'] = int(S_row.pop(0))
    md['scan_command'] = S_row.pop(0)
    md['scan_args'] = {k: v for k, v in zip(
        ['scan_motor', 'start', 'stop', 'num', 'time'], S_row)}
    md['motors'] = [md['scan_args']['scan_motor']]
    # Not sure how to add the 'detectors' line to the RunStart
    # md['detectors'] =
    md['motor_values'] = []
    md['geometry'] = []
    line_hash_mapping = {
        '#G': 'geometry',
        '#P': 'motor_values',
    }
    for line in raw_scan_data[1:]:
        if line.startswith('#'):
            # split the line into the "line_type" which tells us the type of
            # information that this line contains and the "line_contents" which
            # tells us
            line_type, line_contents = line.split(' ', 1)
            if line_type in spec_line_parser:
                attr, func = spec_line_parser[line_type]
                md[attr] = func(line_contents)
            elif line_type == '#L':
                # It is critical that this line be split on *two* spaces
                col_names = line_contents.split('  ')
                md['x_name'] = col_names[0]
                md['col_names'] = col_names
            elif line_type[:2] in line_hash_mapping:
                # These lines are numbered like #G0, #G1, #G2, etc..
                # We need to just grab the #G part, so take only the first two
                # elements with line_type[:2]
                vals = [float(v) for v in line_contents.split()]
                md[line_hash_mapping[line_type[:2]]].extend(vals)
    # iterate through the lines again and capture just the scan data
    scan_data = np.asarray([line.split() for line in raw_scan_data
                           if not line.startswith('#') if line])
    try:
        x = scan_data[:,0]
    except IndexError:
        # there must be no scan data...
        return md, None

    scan_data = pd.DataFrame(
        data=scan_data, columns=md['col_names'], index=x, dtype=float)
    scan_data.index.name = md['x_name']
    return md, scan_data


class Specfile(object):
    """Object model for a spec file.

    Slicing works on the scan number.

    Attributes
    ----------
    filename : str
        The filename of the spec file that this model represents
    header : list
    parsed_header : dict
        Dictionary of metadata that is contained in the header
    scans : dict
        Dictionary of suitcase.spec.Specscan objects where the keys are the
        scan number

    Usage
    -----
    >>> sf = Specfile('/path/to/specfile')
    >>> scan1 = sf[1]
    >>> scan1_data = scan1.scan_data

    Notes
    -----
    known keys for parsed_header include:
        - motor_human_names
        - motor_spec_names
        - detector_human_names
        - detector_spec_names

    See the source of `suitcase.spec.parse_spec_scan()` and
    `suitcase.spec.spec_line_parser` for more complete information
    """
    def __init__(self, filename):
        """

        Parameters
        ----------
        filename : str or file handle
            The filename of the spec file that this model represents or a file
            handle to an open file
        """
        super(Specfile, self).__init__()
        if isinstance(filename, six.string_types):
            self.filename = os.path.abspath(filename)
            with open(self.filename, 'r') as f:
                scan_data = f.read().split('#S')
        else:
            self.filename = filename
            scan_data = self.filename.read().split('#S')
        # reintroduce '#S' to the start of each scan row
        scan_data = [scan_data[0]] + ['#S' + d for d in scan_data[1:]]
        scan_data = [section.split('\n') for section in scan_data]
        self.header = scan_data.pop(0)
        # parse header
        self.parsed_header = parse_spec_header(self.header)
        self.scans = {}
        for scan in scan_data:
            sid = int(scan[0].split()[1])
            self.scans[sid] = Specscan(self, scan)

    def __getitem__(self, key):
        if isinstance(key, slice) or (isinstance(key, int) and key < 0):
            return list(self)[key]

        return self.scans[key]

    def __len__(self):
        return len(self.scans)

    def __iter__(self):
        return (self.scans[sid] for sid in sorted(self.scans.keys()))

    def __repr__(self):
        return "Specfile('{}')".format(self.filename)

    def __eq__(self, obj):
        if not isinstance(obj, type(self)):
            return False
        return all([s1 == s2 for s1, s2 in zip(self, obj)])

    def __str__(self):
        return """
{0}
{1}
{2} scans
user: {3}""".format(self.filename, self.parsed_header['time'], len(self),
                    self.parsed_header['user'])


class Specscan(object):
    """Object that contains data from a single spec scan.

    A scan corresponds to one invocation of a spec command

        `dscan mtr start stop step acq_time`

    Attributes
    ----------
    specfile : suitcase.spec.Specfile
        The parent specfile object
    raw_scan_data : list
        List of the lines that comprise the spec scan
    md : dict
        Dictionary containing all the metadata that we know how to parse.
    scan_data : pandas.DataFrame
        Dataframe-ified version of the scan data.  Column names are the #L line
        in the spec file.  Rows are indexed starting at 0
    """
    def __init__(self, specfile, raw_scan_data):
        super(Specscan, self).__init__()
        self.specfile = specfile
        self.raw_scan_data = raw_scan_data
        self.md, self.scan_data = parse_spec_scan(self.raw_scan_data)
        for k, v in self.md.items():
            setattr(self, k, v)

    def __repr__(self):
        return "{}[{}]".format(repr(self.specfile), self.scan_id)

    def __len__(self):
        return len(self.scan_data)

    def __eq__(self, obj):
        if not isinstance(obj, type(self)):
            return False
        return hash(self) == hash(obj)

    def __lt__(self, obj):
        return self.scan_id < obj.scan_id

    def __str__(self):
        return """Scan {}
{}
{} points in the scan
{} """.format(self.scan_id, self.scan_command + " " + " ".join(self.scan_args),
              len(self), self.time_from_date)

    def __hash__(self):
        return hash('\n'.join(self.raw_scan_data))

###############################################################################
# Spec to document code
###############################################################################

@singledispatch
def spec_to_document(specfile, scan_ids=None, validate=False,
                     check_in_broker=False):
    """Convert one or more scans in a specfile into documents

    Parameters
    ----------
    specfile : str
        The path to the spec file that should be loaded
    scan_ids : int or iterable, optional
        The scan ids that should be converted into documents
    validate : bool, optional
        Whether or not to use jsonschema validation on the documents that are
        being created. Defaults to False
    check_in_broker : bool, optional
        Defaults to False.
        True/False: Do/Don't check in in metadatstore for the presence of
                    these documents. If a document exists in metadatastore,
                    replace the generated document with the one already in
                    metadatastore.

    Yields
    ------
    documents
        Yields a stream of documents in order:
        1. RunStart
        2. baseline Descriptor
        3. baseline Event
        4. primary Descriptor
        5. primary Events (until rows in spec scan are exhausted)
        6. RunStop

    Notes
    -----
    If multiple scan_ids are requested, then documents will be emitted in the
    order listed above until all scan_ids have been processed
    """
    raise ValueError("Variable `specfile` is of type {0}. We only support"
                     "strings, suitcase.spec.Specfile or "
                     "suitcase.spec.Specscan objects here."
                     "".format(specfile))


@spec_to_document.register(str)
def _(specfile, scan_ids=None, validate=False, check_in_broker=False):
    """
    Handle the case when type(specfile) == str
    """
    specfile = Specfile(filename=specfile)
    for document_name, document in spec_to_document(
            specfile, scan_ids=scan_ids, validate=validate,
            check_in_broker=check_in_broker):
      yield document_name, document


@spec_to_document.register(Specfile)
def _(specfile, scan_ids=None, validate=False, check_in_broker=False):
    """
    Handle the case when type(specfile) == Specfile
    """
    # grab the scans that we want to convert
    if scan_ids is None:
        scans_to_process = specfile
    else:
        try:
            scan_ids[0]
        except TypeError:
            scan_ids = [scan_ids]
        finally:
            scans_to_process = [specfile[sid] for sid in scan_ids]
    for scan in scans_to_process:
        try:
            for document_name, document in spec_to_document(
                    scan, scan_ids=scan_ids, validate=validate,
                    check_in_broker=check_in_broker):
                yield document_name, document
        except NotImplementedError as e:
            warnings.warn(e.args[0])


@spec_to_document.register(Specscan)
def _(specfile, scan_ids=None, validate=False, check_in_broker=False):
    """
    Handle the case when type(specfile) == Specscan
    """
    for document_name, document in specscan_to_document_stream(
            specfile, validate=validate, check_in_broker=check_in_broker):
        yield document_name, document

def specscan_to_document_stream(scan, validate=False, check_in_broker=False):
    """
    Turn a single spec scan into a document stream

    Parameters
    ----------
    scan : Specscan
    validate : bool
        True/False: Do/Don't validate the documents against their json schema
                    as defined in `event_model`
    check_in_broker : bool
        True/False: Do/Don't check to see if the documents already exist in
                    metadatastore

    Yields
    -------
    document_name : {event_model.DocumentNames}
        One of the values of the `DocumentNames` enum
    document_dict : doct.Document
        A document that is identical to one that would come from the
        metadatastore find_* functions. It may or may not already exist in
        metadatastore.  You will need to call find_* yourself to determine
        if it does exist
    """
    if mdsc is None and validate:
        raise NotImplementedError(
            "It is not possible to use the `check_in_broker=True` unless you "
            "have metadatastore installed. Please re-run this function with "
            "`check_in_broker=False` or install metadatastore."
        )
    # do the conversion!
    kw = {'validate': validate, 'check_in_broker': check_in_broker}
    document_name, document = next(to_run_start(scan, **kw))
    start_uid = document['uid']
    # yield the start document
    yield document_name, document
    # yield the baseline descriptor and its event
    for document_name, document in to_baseline(scan, start_uid, **kw):
        yield document_name, document
    for document_name, document in to_events(scan, start_uid, **kw):
        # yield the descriptor and events
        yield document_name, document
    # make sure the run was finished before it was stopped
    # yield the stop document
    gen = to_stop(scan, start_uid, **kw)
    document_name, document = next(gen)
    yield document_name, document


def _validate(doc_name, doc_dict):
    """Run the documents through jsonschema validation

    Parameters
    ----------
    doc_name : str
        One of the keys in event_model.schemas
    doc_dict : dict
        The dictionary that is supposedly of `doc_name` type
    """
    jsonschema.validate(doc_dict, event_model.schemas[doc_name])


def _get_timestamp(dt):
    """
    Helper function to utilize the new datetime.timestamp() functionality
    in python 3 with the old method that is required for python 2

    Parameters
    ----------
    dt : datetime.datetime
        Datetime object

    Returns
    -------
    timestamp : float
        Total seconds since 1970
    """
    try:
        # python 3 :-D
        return dt.timestamp()
    except AttributeError:
        # python 2 :-(
        return (dt - datetime(1970, 1, 1)).total_seconds()


_find_map = {
    event_model.DocumentNames.start: "find_run_starts",
    event_model.DocumentNames.stop: "find_run_stops",
    event_model.DocumentNames.descriptor: "find_descriptors",
    event_model.DocumentNames.event: "find_events",
}


def _check_and_update_document(doc_name, doc_dict):
    """
    Check to see if the document already exists in metadatastore and

    Parameters
    ----------
    doc_name : {event_model.DocumentNames}
        One of the values in the event_model.DocumentNames enum
    doc_dict : dict
        The dictionary that corresponds to a document

    Returns
    -------
    dict
        A dictionary that contains all the keys that a document that came out
        of bluesky or metadatastore would contain.
    """
    find_func = getattr(mdsc, _find_map[doc_name])
    if doc_name == event_model.DocumentNames.start:
        find_kwargs = {'hashed_scandata': doc_dict['hashed_scandata']}
    elif (doc_name == event_model.DocumentNames.descriptor or
          doc_name == event_model.DocumentNames.stop):
        find_kwargs = {'hashed_scandata': doc_dict['hashed_scandata'],
                       'run_start': doc_dict['run_start']}
    elif doc_name == event_model.DocumentNames.event:
        # need to special case the event because we can't add any extra keys
        # into the event document
        find_kwargs = {'descriptor': doc_dict['descriptor'],
                       'seq_num': doc_dict['seq_num']}
    # do the actual search for the documents
    documents = list(find_func(**find_kwargs))
    # handle the case when the document does not exist in metadatastore
    if not documents:
        logger.debug('%s document does not exist in mds', doc_name)
        return doc_dict
    # handle the (hopefully never) case when there are more than one documents
    # that match the query
    elif len(documents) > 1:
        raise ValueError('There were {} documents found for the search with '
                         'kwargs={}'.format(len(documents), find_kwargs))
    mds_doc, = documents

    # there is only one document that was returned. Make sure it matches the
    # one we think it should
    common_items = ['uid', 'time']
    keys_that_might_differ = {
        event_model.DocumentNames.start: common_items,
        event_model.DocumentNames.descriptor: common_items + ['run_start'],
        event_model.DocumentNames.event: common_items + ['descriptor'],
        event_model.DocumentNames.stop: common_items + ['run_start'],
    }

    d1 = {k: v for k, v in doc_dict.items()
          if k not in keys_that_might_differ[doc_name]}
    d2 = {k: v for k, v in mds_doc.items()
          if k not in keys_that_might_differ[doc_name]}

    assert d1 == d2
    doc_copy = copy.deepcopy(doc_dict)
    for k in keys_that_might_differ[doc_name]:
        doc_copy.update({k: mds_doc.get(k, doc_dict[k])})


    logger.debug("%s document already exists in mds. Returning the document "
                 "from metadatastore instead")
    return doc_copy


def to_run_start(specscan, validate=False, check_in_broker=False, **md):
    """Convert a Specscan object into a RunStart document

    Parameters
    ----------
    specscan : suitcase.spec.Specscan
    validate : bool, optional
        Whether or not to use jsonschema validation on the document that is
        being created. Defaults to False
    **md : dict
        Any extra metadata to insert into the RunStart document. Note that
        any values in this **md dict will take precedence over the default
        values that come from the Specscan object

    Yields
    ------
    document_name : str
        'start' is what is yielded here
    run_start_dict : dict
        The RunStart document that can be inserted into metadatastore or
        processed with a callback from the ``callbacks`` project.
    """
    if specscan.scan_command not in _SPEC_SCAN_NAMES:
        msg = ("Cannot convert scan {0} to a document stream. {1} is not a "
               "scan that I know how to convert.  I can only convert {2}"
               "".format(specscan.scan_id, specscan.scan_command,
                         _SPEC_SCAN_NAMES))
        raise NotImplementedError(msg)
    plan_name = _BLUESKY_PLAN_NAMES[_SPEC_SCAN_NAMES.index(specscan.scan_command)]
    run_start_dict = {
        'time': _get_timestamp(specscan.time_from_date),
        'scan_id': specscan.scan_id,
        'uid': str(uuid.uuid4()),
        'specpath': specscan.specfile.filename,
        'owner': specscan.specfile.parsed_header['user'],
        'plan_args': specscan.scan_args,
        'motors': [specscan.col_names[0]],
        'plan_name': plan_name,
        'group': 'SpecToDocumentConverter',
        'beamline_id': 'SpecToDocumentConverter',
        'hashed_scandata': hash(specscan),
    }
    run_start_dict.update(**md)
    if validate:
        _validate(event_model.DocumentNames.start, run_start_dict)
    if check_in_broker:
        run_start_dict = _check_and_update_document(
            event_model.DocumentNames.start, run_start_dict)
    yield event_model.DocumentNames.start, doct.Document('RunStart',
                                                         run_start_dict)


def to_baseline(specscan, start_uid, validate=False, check_in_broker=False):
    """Convert a Specscan object into a baseline Descriptor and Event

    Parameters
    ----------
    specscan : suitcase.spec.Specscan
    start_uid : str
        The uid of the RunStart document that these baseline documents should
        be associated with.
    validate : bool, optional
        Whether or not to use jsonschema validation on the document that is
        being created. Defaults to False

    Yields
    ------
    document_name : str
        'descriptor' followed by 'event'
    baseline_dict : dict
        The baseline EventDescriptor followed by the baseline Event. These
        documents can be inserted into into metadatastore or processed with a
        callback from the ``callbacks`` project.
    """
    timestamp = _get_timestamp(specscan.time_from_date)
    data_keys = {}
    data = {}
    timestamps = {}
    for obj_name, human_name, value in zip(
            specscan.specfile.parsed_header['motor_spec_names'],
            specscan.specfile.parsed_header['motor_human_names'],
            specscan.motor_values):
        data_keys[obj_name] = {'dtype': 'number',
                               'shape': [],
                               'source': human_name,
                               'object_name': obj_name,
                               'precision': -1,
                               'units': 'N/A'}
        data[obj_name] = value
        timestamps[obj_name] = timestamp
    try:
        specscan.hkl
    except AttributeError:
        warnings.warn("scan {0} does not have hkl values"
                      "".format(specscan.scan_id))
    else:
        data_keys.update({k: {'dtype': 'number',
                              'shape': [],
                              'source': k,
                              'object_name': k,
                              'precision': -1,
                              'units': 'N/A'} for k in 'hkl'})
        data.update({k: v for k, v in zip('hkl', specscan.hkl)})
        timestamps.update({k: timestamp for k in 'hkl'})
    descriptor = dict(run_start=start_uid, data_keys=data_keys,
                      time=timestamp, uid=str(uuid.uuid4()),
                      name='baseline',
                      hashed_scandata=hash(str(
                          specscan.specfile.parsed_header['motor_spec_names'] +
                          specscan.specfile.parsed_header['motor_human_names']
                      )))
    if validate:
        _validate(event_model.DocumentNames.descriptor, descriptor)
    if check_in_broker:
        descriptor = _check_and_update_document(
            event_model.DocumentNames.descriptor, descriptor)
    yield (event_model.DocumentNames.descriptor,
           doct.Document('EventDescriptor', descriptor))
    event = dict(descriptor=descriptor['uid'], seq_num=0, time=timestamp,
                 data=data, timestamps=timestamps, uid=str(uuid.uuid4()))
    if validate:
        _validate(event_model.DocumentNames.event, event)
    if check_in_broker:
        event = _check_and_update_document(
            event_model.DocumentNames.event, event)
    yield event_model.DocumentNames.event, doct.Document('Event', event)


def to_events(specscan, start_uid, validate=False, check_in_broker=False):
    """Convert a Specscan object into a Descriptor and Event documents

    Parameters
    ----------
    specscan : suitcase.spec.Specscan
    start_uid : str
        The uid of the RunStart document that these baseline document should be
        associated with.
    validate : bool, optional
        Whether or not to use jsonschema validation on the document that is
        being created. Defaults to False

    Yields
    ------
    document_name : str
        'descriptor' followed by 'event'
    baseline_dict : dict
        The EventDescriptor followed by the stream of Events. These documents
        can be inserted into into metadatastore or processed with a callback
        from the ``callbacks`` project.
    """
    timestamp = _get_timestamp(specscan.time_from_date)
    data_keys = {col: {'dtype': 'number',
                       'shape': [],
                       'source': col,
                       'object_name': col,
                       'precision': -1,
                       'units': 'N/A'}
                 for col in specscan.col_names}
    descriptor = dict(run_start=start_uid, data_keys=data_keys,
                      time=timestamp, uid=str(uuid.uuid4()),
                      hashed_scandata=hash(str(specscan.col_names)))
    if validate:
        _validate(event_model.DocumentNames.descriptor, descriptor)
    if check_in_broker:
        descriptor = _check_and_update_document(
            event_model.DocumentNames.descriptor, descriptor)
    yield (event_model.DocumentNames.descriptor,
           doct.Document('EventDescriptor', descriptor))
    timestamps = {col: timestamp for col in specscan.col_names}
    for seq_num, (x, row_series) in enumerate(specscan.scan_data.iterrows()):
        data = {col: data for col, data in zip(row_series.index, row_series[:])}
        event = dict(data=data, descriptor=descriptor['uid'],
                     seq_num=seq_num, time=timestamp + data['Epoch'],
                     timestamps=timestamps, uid=str(uuid.uuid4()))
        if validate:
            _validate(event_model.DocumentNames.descriptor, descriptor)
        if check_in_broker:
            event = _check_and_update_document(
                event_model.DocumentNames.event, event)
        yield event_model.DocumentNames.event, doct.Document('Event',
                                                             event)


def to_stop(specscan, start_uid, validate=False, check_in_broker=False, **md):
    """Convert a Specscan object into a Stop document

    Parameters
    ----------
    specscan : suitcase.spec.Specscan
    start_uid : str
        The uid of the RunStart document that this Stop document should
        be associated with.
    validate : bool, optional
        Whether or not to use jsonschema validation on the document that is
        being created. Defaults to False
    **md : dict
        Any extra metadata to insert into the RunStart document. Note that
        any values in this **md dict will take precedence over the default
        values that come from the Specscan object

    Yields
    ------
    document_name : str
        'stop'
    stop_dict : dict
        The RunStop document that can be inserted into into metadatastore or
        processed with a callback from the ``callbacks`` project.
    """

    md['exit_status'] = 'success'
    actual_events = len(specscan.scan_data)
    if specscan.scan_command == _NOT_IMPLEMENTED_SCAN:
        # not sure how many events we should have. Assume it exited correctly
        expected_events = actual_events
    elif specscan.scan_command in _SCANS_WITHOUT_MOTORS:
        expected_events = 1
    else:
        expected_events = int(specscan.scan_args['num']) + 1

    if actual_events != expected_events:
        md['reason'] = ('Expected events: {}. Actual events: {}'
                        ''.format(expected_events, actual_events))
        md['exit_status'] = 'abort'
        warnings.warn('scan %s only has %s/%s points. Assuming scan was '
                      'aborted. start_uid=%s' % (specscan.scan_id,
                                                 actual_events,
                                                 expected_events,
                                                 start_uid))
    timestamp = _get_timestamp(specscan.time_from_date)
    md['hashed_scandata'] = hash(specscan)
    stop = dict(run_start=start_uid, time=timestamp, uid=str(uuid.uuid4()),
                **md)
    if validate:
        _validate(event_model.DocumentNames.stop, stop)
    if check_in_broker:
        stop = _check_and_update_document(event_model.DocumentNames.stop, stop)
    yield event_model.DocumentNames.stop, doct.Document('RunStop', stop)


###############################################################################
# Document to Spec code
###############################################################################


env = jinja2.Environment()


_SPEC_FILE_HEADER_TEMPLATE = env.from_string("""#F {{ filename }}
#E {{ unix_time }}
#D {{ readable_time }}
#C {{ owner }}  User = {{ owner }}
#O0 {{ positioner_variable_sources | join ('  ') }}
#o0 {{ positioner_variable_names | join(' ') }}""")


_DEFAULT_POSITIONERS = {
    'data_keys':
        {'Science':
             {'dtype': 'number', 'shape': [], 'source': 'SOME:RANDOM:PV'},
         'Data':
             {'dtype': 'number', 'shape': [], 'source': 'SOME:OTHER:PV'}}}


def to_spec_file_header(start, filepath, baseline_descriptor=None):
    """Generate a spec file header from some documents

    Parameters
    ----------
    start : Document or dict
        The RunStart that is emitted by the bluesky.run_engine.RunEngine or
        something that is compatible with that format
    filepath : str
        The filename of this spec scan. Will use os.path.basename to find the
        filename
    baseline_descriptor : Document or dict, optional
        The 'baseline' Descriptor document that is emitted by the RunEngine
        or something that is compatible with that format.
        Defaults to the values in suitcase.spec._DEFAULT_POSITIONERS

    Returns
    -------
    str
        The formatted SPEC file header. You probably want to split on "\n"
    """
    if baseline_descriptor is None:
        baseline_descriptor = _DEFAULT_POSITIONERS
    md = {}
    md['owner'] = start['owner']
    md['positioner_variable_names'] = sorted(
            list(baseline_descriptor['data_keys'].keys()))
    md['positioner_variable_sources'] = [
        baseline_descriptor['data_keys'][k]['source'] for k
        in md['positioner_variable_names']]
    md['unix_time'] = int(start['time'])
    md['readable_time'] = to_spec_time(datetime.fromtimestamp(md['unix_time']))
    md['filename'] = os.path.basename(filepath)
    return _SPEC_FILE_HEADER_TEMPLATE.render(md)


_SPEC_1D_COMMAND_TEMPLATE = env.from_string("{{ plan_name }} {{ scan_motor }} {{ start }} {{ stop }} {{ num }} {{ time }}")

_SCANS_WITHOUT_MOTORS = ['ct']
_SCANS_WITH_MOTORS = ['ascan', 'dscan']
_SPEC_SCAN_NAMES = _SCANS_WITH_MOTORS + _SCANS_WITHOUT_MOTORS
_NOT_IMPLEMENTED_SCAN = 'Other'
_BLUESKY_PLAN_NAMES = ['dscan', 'ascan', 'ct']

_SPEC_SCAN_HEADER_TEMPLATE = env.from_string("""

#S {{ scan_id }} {{ command }}
#D {{ readable_time }}
#T {{ acq_time }}  (Seconds)
#P0 {{ positioner_positions | join(' ')}}
#N {{ num_columns }}
#L {{ motor_name }}  Epoch  Seconds  {{ data_keys | join('  ') }}
""")


def _get_acq_time(start, default_value=-1):
    """Private helper function to extract the heuristic count time

    The SPEC-style plans inject a heuristic 'count_time' (which has
    different meanings in different contexts) as a top-level key in the
    RunStart document.

    Parameters
    ----------
    start : Document or dict
        The RunStart document emitted by the bluesky RunEngine or a dictionary
        that has compatible information
    default_value : int, optional
        The default acquisition time. Defaults to -1
    """
    time = start.get('count_time', default_value)
    if time is None:
        # 'None' is not legal in spec
        time = default_value
    return time


def _get_plan_name(start):
    plan_name = start['plan_name']
    if plan_name not in _BLUESKY_PLAN_NAMES:
        warnings.warn(
            "Do not know how to represent {} in SPEC. If you would like this "
            "feature, request it at https://github.com/NSLS-II/bluesky/issues. "
            "Until this feature is implemented, we will be using the sequence "
            "number as the motor position".format(plan_name))
        return _NOT_IMPLEMENTED_SCAN
    return _SPEC_SCAN_NAMES[_BLUESKY_PLAN_NAMES.index(plan_name)]


def _get_motor_name(start):
    plan_name = _get_plan_name(start)
    if plan_name == _NOT_IMPLEMENTED_SCAN or plan_name in _SCANS_WITHOUT_MOTORS:
        return 'seq_num'
    motor_name = start['motors']
    # We only support a single scanning motor right now.
    if len(motor_name) > 1:
        warnings.warn(
            "Your scan has {0} scanning motors. They are {1}. Conversion to a"
            "specfile does not understand what to do with multiple scanning. "
            "Please request this feature at "
            "https://github.com/NSLS-II/suitcase/issues Until this feature is "
            "implemented, we will be using the sequence number as the motor "
            "position".format((len(motor_name), motor_name)))
        return 'seq_num'
    return motor_name[0]


def _get_motor_position(start, event):
    plan_name = _get_plan_name(start)
    # make sure we are trying to get the motor position for an implemented scan
    if plan_name == _NOT_IMPLEMENTED_SCAN or plan_name in _SCANS_WITHOUT_MOTORS:
        return event['seq_num']
    motor_name = _get_motor_name(start)
    # make sure we have a motor name that we can get data for. Otherwise we use
    # the sequence number of the event
    if motor_name == 'seq_num':
        return event['seq_num']
    # if none of the above conditions are met, we can get a motor value. Thus we
    # return the motor value in the event
    return event['data'][motor_name]


def _get_scan_data_column_names(start, primary_descriptor):
    motor_name = _get_motor_name(start)
    # List all scalar fields, excluding the motor (x variable).
    read_fields = sorted(
        [k for k, v in primary_descriptor['data_keys'].items()
         if (v['object_name'] != motor_name and not v['shape'])])
    return read_fields


def to_spec_scan_header(start, primary_descriptor, baseline_event=None):
    """Convert the RunStart, "primary" Descriptor and the "baseline" Event
    into a spec scan header

    Parameters
    ----------
    start : Document or dict
        The RunStart document emitted by the bluesky RunEngine or a dictionary
        that has compatible information
    primary_descriptor : Document or dict
        The Descriptor that corresponds to the main event stream
    baseline_event : Document or dict, optional
        The Event that corresponds to the mass reading of motors before the
        scan begins.
        Default value is `-1` for each of the keys in
        `suitcase.spec._DEFAULT_POSITIONERS`

    Returns
    -------
    str
        The formatted SPEC scan header. You probably want to split on "\n"
    """
    if baseline_event is None:
        baseline_event = {
            'data':
                {k: -1 for k in _DEFAULT_POSITIONERS['data_keys']}}
    md = {}
    md['scan_id'] = start['scan_id']
    scan_command = _get_plan_name(start)
    motor_name = _get_motor_name(start)
    acq_time = _get_acq_time(start)
    # can only grab start/stop/num if we are a dscan or ascan.
    if (scan_command == _NOT_IMPLEMENTED_SCAN or
            scan_command in _SCANS_WITHOUT_MOTORS):
        command_args = []
    else:
        command_args = [start['plan_args'][k]
                        for k in ('start', 'stop', 'num')]
    command_list = ([scan_command, motor_name] + command_args + [acq_time])
    # have to ensure all list elements are strings or join gets angry
    md['command'] = ' '.join([str(s) for s in command_list])
    md['readable_time'] = to_spec_time(datetime.fromtimestamp(start['time']))
    md['acq_time'] = acq_time
    md['positioner_positions'] = [
        v for k, v in sorted(baseline_event['data'].items())]
    md['data_keys'] = _get_scan_data_column_names(start, primary_descriptor)
    md['num_columns'] = 3 + len(md['data_keys'])
    md['motor_name'] = _get_motor_name(start)
    return _SPEC_SCAN_HEADER_TEMPLATE.render(md)


_SPEC_EVENT_TEMPLATE = env.from_string("""
{{ motor_position }}  {{ unix_time }} {{ acq_time }} {{ values | join(' ') }}""")


def to_spec_scan_data(start, primary_descriptor, event):
    md = {}
    md['unix_time'] = int(event['time'])
    md['acq_time'] = _get_acq_time(start)
    md['motor_position'] = _get_motor_position(start, event)
    data_keys = _get_scan_data_column_names(start, primary_descriptor)
    md['values'] = [event['data'][k] for k in data_keys]
    return _SPEC_EVENT_TEMPLATE.render(md)


# Copied from bluesky.  This should probably be replaced by an import from
# the callbacks package that will be created from the bluesky.callbacks
# subpackage at some point.
class CallbackBase:
    def __call__(self, name, doc):
        """
        Dispatch to methods expecting particular doc types.
        """
        return getattr(self, name)(doc)

    def event(self, doc):
        pass

    def bulk_events(self, doc):
        pass

    def descriptor(self, doc):
        pass

    def start(self, doc):
        pass

    def stop(self, doc):
        pass


class DocumentToSpec(CallbackBase):
    """Callback to export scalar values to a spec file for viewing

    Expect:

    1. a descriptor named 'baseline'
    2. an event for that descriptor
    3. one primary descriptor
    4. events for that one primary descriptor

    Other documents can be received before, between, and after, but
    these must be received and in this order.

    Example
    -------
    It is suggested to put this in the ipython profile:
    >>> from suitcase.spec import DocumentToSpec
    >>> document_to_spec_callback =  DocumentToSpec(os.path.expanduser('~/specfiles/test.spec'))
    >>> gs.RE.subscribe('all', document_to_spec_callback)
    >>> # Modify the spec file location like this:
    >>> # document_to_spec_callback.filepath = '/some/new/filepath.spec'

    Notes
    -----
    1. `Reference <https://github.com/certified-spec/specPy/blob/master/doc/specformat.rst>`_
        for the spec file format.
    2. If there is more than one primary descriptor, the behavior of this spec
       callback is undefined.  Please do not use this callback with more than
       one descriptor.
    """
    def __init__(self, specpath):
        """
        Parameters
        ----------
        specpath : str
            The location on disk where you want the specfile to be written
        """
        self.specpath = os.path.abspath(specpath)
        self.pos_names = ["No", "Positioners", "Were", "Given"]
        self.positions = ["-inf", "-inf", "-inf", "-inf"]
        self._start = None
        self._baseline_descriptor = None
        self._baseline_event = None
        self._primary_descriptor = None
        self._has_not_written_scan_header = True
        self._num_events_received = 0
        self._num_baseline_events_received = 0

    def start(self, doc):
        """
        Stash the start document and reset the internal state
        """
        logger.debug("start document received")
        self._start = doc
        self._baseline_descriptor = None
        self._baseline_event = None
        self._primary_descriptor = None
        self._has_not_written_scan_header = True
        self._num_events_received = 0
        self._num_baseline_events_received = 0

    def _write_new_header(self):
        if not os.path.exists(self.specpath):
            logger.debug("Writing new spec file header")
            header = to_spec_file_header(self._start, self.specpath,
                                         self._baseline_descriptor)
            with open(self.specpath, 'w') as f:
                f.write(header)
        # for now assume we don't need to write a new header.  Will revisit
        # when someone wants to be able to do this.

    def descriptor(self, doc):
        if doc.get('name') == 'baseline':
            logger.debug("baseline descriptor received")
            # if this is the baseline descriptor, we might need to write a
            # new file header
            self._baseline_descriptor = doc
        elif self._primary_descriptor:
            # we already have a primary descriptor, why are we getting
            # another one?
            err_msg = (
                "The DocumentToSpec callback is not designed to handle more "
                "than one event stream.  If you need this functionality, please "
                "request it at https://github.com/NSLS-II/suitcase/issues. "
                "Until that time, this DocumentToSpec callback will raise a "
                "NotImplementedError if you try to use it with two event "
                "streams.")
            warnings.warn(err_msg)
            raise NotImplementedError(err_msg)
        else:
            logger.debug("primary descriptor received")
            self._primary_descriptor = doc

    def event(self, doc):
        if (self._baseline_descriptor and
                    doc['descriptor'] == self._baseline_descriptor['uid']):
            logger.debug("Received baseline event document")
            self._num_baseline_events_received += 1
            self._baseline_event = doc
            return
        # Write the scan header as soon as we get the first event.  If it is
        # not the baseline event, then sorry! You need to give me that before
        # any primary events.
        if self._has_not_written_scan_header:
            if self._baseline_event is None:
                err_msg = (
                    "No baseline event was received. If you want the motor "
                    "positions for non-scanning motors, you need to implement "
                    "your scans with baseline events being recorded. If you "
                    "need help doing this, please request help at "
                    "https://github.com/NSLS-II/Bug-Reports/issues")
                warnings.warn(err_msg)
            # maybe write a new header if there is not one already
            self._write_new_header()
            # write the scan header with whatever information we currently have
            scan_header = to_spec_scan_header(self._start,
                                              self._primary_descriptor,
                                              self._baseline_event)
            with open(self.specpath, 'a') as f:
                f.write(scan_header)
            self._has_not_written_scan_header = False

        if doc['descriptor'] != self._primary_descriptor['uid']:
            err_msg = (
                "The DocumentToSpec callback is not designed to handle more "
                "than one event stream.  If you need this functionality, please "
                "request it at https://github.com/NSLS-II/suitcase/issues. "
                "Until that time, this DocumentToSpec callback will raise a "
                "NotImplementedError if you try to use it with two event "
                "streams.")
            warnings.warn(err_msg)
            raise NotImplementedError(err_msg)
        # We must be receiving a primary event
        logger.debug("Received primary event document")
        self._num_events_received += 1
        # now write the scan data line
        scan_data_line = to_spec_scan_data(self._start,
                                           self._primary_descriptor, doc)
        with open(self.specpath, 'a') as f:
            f.write(scan_data_line + '\n')

    def stop(self, doc):
        logger.debug("Received stop document")
        msg = '\n'
        if doc['exit_status'] != 'success':
            msg += ('#C Run exited with status: {exit_status}. Reason: '
                    '{reason}'.format(**doc))
        with open(self.specpath, 'a') as f:
            f.write(msg)


# ##########################################################################
# Code for inserting documents into metadatastore
# ##########################################################################
_inserted = namedtuple('inserted', ['doc_name', 'uid', 'inserted'])
def insert_specscan_into_broker(specscan, validate=False,
                                check_in_broker=True):
    """
    Insert a single spec scan into the databroker

    Parameters
    ----------
    specscan : Specscan
        The Specscan object to insert into the databroker
    validate : bool, optional
        Defaults to False.
        True/False: Do/Don't validate the document dict against the json
                    schema defined in event_model
    check_in_broker : bool, optional
        Defaults to True.
        True/False: Do/Don't check to see if the document already exists in
                    metadatastore.  If it does, the document will be replaced
                    with the one that is already in metadatastore

    Returns
    -------
    list
        List of `_inserted` namedtuples that provide some information regarding
        which documents were inserted and which already existed.
    """
    if mdsc is None:
        raise RuntimeError("metadatastore is not available. This function is "
                           "not usable.")
    doc_gen = specscan_to_document_stream(specscan, validate=validate,
                                          check_in_broker=check_in_broker)
    uids = []
    for doc_name, doc in doc_gen:
        # insert the document
        find_func = getattr(mdsc, _find_map[doc_name])
        documents = list(find_func(uid=doc.uid))
        inserted = False
        if not documents:
            # insert the document
            logger.debug("inserting {} with uid={}".format(doc_name, doc.uid))
            # metadatastore does not know how to handle the _name
            doc_dict = dict(doc)
            doc_dict.pop('_name', None)
            mdsc.insert(doc_name.name, doc_dict)
            inserted = True
        else:
            # there is only one document.  It is not possible for mds to return
            # more than one document with the same uid
            logger.debug("{} with uid: {} already exists in mds".format(
                doc_name, doc.uid))
        uids.append(_inserted(doc_name, doc.uid, inserted))
    return uids


def insert_specfile_into_broker(specfile, validate=False,
                                check_in_broker=True):

    """
    Insert all (most) scans in a specfile into the databroker

    Scans whose type is not in the module-level variable _BLUESKY_PLAN_NAMES
    will not be inserted into the databroker.

    Parameters
    ----------
    specfile : Specfile
        The Specfile object to insert into the databroker
    validate : bool
        Defaults to False.
        True/False: Do/Don't validate the document dict against the json
                    schema defined in event_model
    check_in_broker : bool
        Defaults to True.
        True/False: Do/Don't check to see if the document already exists in
                    metadatastore.  If it does, the document will be replaced
                    with the one that is already in metadatastore

    Returns
    -------
    suceeded : list
        List of tuples of the scan object and a list of ``_inserted`` namedtuples
        that contain the document name, the document uid and whether or not
        the insertion was successful for every document that was inserted into
        metadatastore
    failed : list
        List of tuples of the scan object and the error that was raised for
        any scans that failed to insert

    Notes
    -----
    see `summarize_insertion` for a helper function to format the return values
    of this function
    """
    suceeded = []
    failed = []
    for scan in specfile:
        try:
            uids = insert_specscan_into_broker(scan, validate=validate,
                                               check_in_broker=check_in_broker)
            suceeded.append((scan, uids))
        except NotImplementedError as e:
            failed.append((scan, e))

    return suceeded, failed


def summarize_insertion(suceeded, failed):
    """
    Helper function that takes the output of `insert_specfile_into_broker`,
    formats it into easy-to-parse information and prints it out to the console

    Usage
    -----
    >>> ret = insert_specfile_into_broker(specfile_object)
    >>> summarize_insertion(*ret)
    """
    logger.info('{} scans failed to be inserted'.format(len(failed)))
    for scan, exception in failed:
        logger.info('{:>10}: {}'.format(scan.scan_id, exception))
    order = [event_model.DocumentNames.start,
             event_model.DocumentNames.descriptor,
             event_model.DocumentNames.event,
             event_model.DocumentNames.stop]
    table = PrettyTable(field_names=['scan_ids', 'start', 'descriptor',
                                     'event', 'stop'])
    for scan, uids in suceeded:
        docs = defaultdict(list)
        for uid in uids:
            docs[uid.doc_name].append(uid)
        # print out a summary that shows the number of documents that were
        # inserted for each scan, including the number that already existed in
        # metadatastore
        summarize = lambda x: "{}/{}".format(
            len([item for item in x if item.inserted]), len(x))

        row = [scan.scan_id] + [summarize(docs[k]) for k in order]
        table.add_row(row)
    logger.info('{} scans were successfully converted to a document stream'
                '\n'.format(len(table._rows)))
    logger.info('The following table shows the number of documents that '
                'were inserted for each type of document versus the total '
                'number of documents that were created for each scan\n')
    logger.info(table.get_string())
