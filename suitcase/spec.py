"""
`Reference <https://github.com/certified-spec/specPy/blob/master/doc/specformat.rst>`_
for the spec file format.
"""
import uuid
import os
import warnings
from datetime import datetime
import logging
logger = logging.getLogger(__name__)

import six
import numpy as np
import pandas as pd
import jinja2
import doct
import event_model
import jsonschema


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


class Specfile:
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
        return (self.header == obj.header and
                self.parsed_header == obj.parsed_header and
                list(self.scans.keys()) == list(obj.scans.keys()))

    def __str__(self):
        return """
{0}
{1}
{2} scans
user: {3}""".format(self.filename, self.parsed_header['time'], len(self),
                    self.parsed_header['user'])


class Specscan:
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
        return (self.specfile == obj.specfile and
                self.raw_scan_data == obj.raw_scan_data and
                self.md == obj.md and
                self.scan_data.equals(obj.scan_data))

    def __lt__(self, obj):
        return self.scan_id < obj.scan_id

    def __str__(self):
        return """Scan {}
{}
{} points in the scan
{} """.format(self.scan_id, self.scan_command + " " + " ".join(self.scan_args),
              len(self), self.time_from_date)



###############################################################################
# Spec to document code
###############################################################################

def spec_to_document(specfile, scan_ids=None, validate=False):
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
    if isinstance(specfile, str):
        specfile = Specfile(filename=specfile)
    if isinstance(specfile, Specfile):
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
    elif isinstance(specfile, Specscan):
        scans_to_process = [specfile]
    else:
        raise ValueError("Variable `specfile` is of type {0}. We only support"
                         "strings, suitcase.spec.Specfile or "
                         "suitcase.spec.Specscan objects here."
                         "".format(specfile))

    for scan in scans_to_process:
        for document_name, document in specscan_to_document_stream(
                scan, validate=validate):
            yield document_name, document


def specscan_to_document_stream(scan, validate=False):
    # do the conversion!
    document_name, document = next(to_run_start(scan, validate=validate))
    start_uid = document['uid']
    # yield the start document
    yield document_name, document
    # yield the baseline descriptor and its event
    for document_name, document in to_baseline(scan, start_uid,
                                               validate=validate):
        yield document_name, document
    num_events = 0
    for document_name, document in to_events(scan, start_uid,
                                             validate=validate):
        if document_name == 'event':
            num_events += 1
        # yield the descriptor and events
        yield document_name, document
    # make sure the run was finished before it was stopped
    # yield the stop document
    gen = to_stop(scan, start_uid, validate=validate)
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


def to_run_start(specscan, validate=False, **md):
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
        warnings.warn("Cannot convert scan {0} to a document stream. {1} is "
                      "not a scan that I know how to convert.  I can only "
                      "convert {2}".format(specscan.scan_id,
                                           specscan.scan_command,
                                           _SPEC_SCAN_NAMES))
        return
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
        '_name': 'RunStart',
        'group': 'SpecToDocumentConverter',
        'beamline_id': 'SpecToDocumentConverter',
    }
    run_start_dict.update(**md)
    if validate:
        _validate(event_model.DocumentNames.start, run_start_dict)
    yield event_model.DocumentNames.start, doct.Document('RunStart',
                                                         run_start_dict)


def to_baseline(specscan, start_uid, validate=False):
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
        hkl = specscan.hkl
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
                      name='baseline')
    if validate:
        _validate(event_model.DocumentNames.descriptor, descriptor)
    yield (event_model.DocumentNames.descriptor,
           doct.Document('EventDescriptor', descriptor))
    event = dict(descriptor=descriptor['uid'], seq_num=0, time=timestamp,
                 data=data, timestamps=timestamps, uid=str(uuid.uuid4()))
    if validate:
        _validate(event_model.DocumentNames.event, event)
    yield event_model.DocumentNames.event, doct.Document('Event', event)


def to_events(specscan, start_uid, validate=False):
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
                      time=timestamp, uid=str(uuid.uuid4()))
    if validate:
        _validate(event_model.DocumentNames.descriptor, descriptor)
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
        yield event_model.DocumentNames.event, doct.Document('Event',
                                                             event)


def to_stop(specscan, start_uid, validate=False, **md):
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
    stop = dict(run_start=start_uid, time=timestamp, uid=str(uuid.uuid4()),
                **md)
    if validate:
        _validate(event_model.DocumentNames.stop, stop)
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
    """Private helper function to extract the acquisition time from the Start
    document

    Parameters
    ----------
    start : Document or dict
        The RunStart document emitted by the bluesky RunEngine or a dictionary
        that has compatible information
    default_value : int, optional
        The default acquisition time. Defaults to -1
    """
    try:
        return start['plan_args']['time']
    except KeyError:
        return default_value


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
        `
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
