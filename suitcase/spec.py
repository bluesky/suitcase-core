"""
`Reference <https://github.com/certified-spec/specPy/blob/master/doc/specformat.rst>`_
for the spec file format.
"""
import numpy as np
import pandas as pd
import uuid
import os
import warnings
from datetime import datetime
import jinja2

# need callback base from bluesky
try:
    from bluesky.callbacks import CallbackBase
except ImportError as ie:
    warnings.warn("bluesky.callbacks package is required for some spec "
                  "functionality.")

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
        ['scan_motor', 'start', 'stop', 'strides', 'time'], S_row)}
    md['motors'] = [md['plan_args']['scan_motor']]
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
        filename : str
            The filename of the spec file that this model represents
        """
        self.filename = os.path.abspath(filename)
        with open(self.filename, 'r') as f:
            scan_data = f.read().split('#S')
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
        if isinstance(key, slice):
            return list(self)[key]

        return self.scans[key]

    def __len__(self):
        return len(self.scans)

    def __iter__(self):
        return (self.scans[sid] for sid in sorted(self.scans.keys()))

    def __repr__(self):
        return "Specfile('{}')".format(self.filename)

    def __eq__(self, obj):
        if not isinstance(obj, Specfile):
            return False
        return (self.filename == obj.filename and
                self.header == obj.header and
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

def spec_to_document(specfile, scan_ids=None):
    """Convert one or more scans in a specfile into documents

    Parameters
    ----------
    specfile : str
        The path to the spec file that should be loaded
    scan_ids : int or iterable, optional
        The scan ids that should be converted into documents

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
        # do the conversion!
        document_name, document = next(to_run_start(scan))
        start_uid = document['uid']
        # yield the start document
        yield document_name, document
        # yield the baseline descriptor and its event
        for document_name, document in to_baseline(scan, start_uid):
            yield document_name, document
        num_events = 0
        for document_name, document in to_events(scan, start_uid):
            if document_name == 'event':
                num_events += 1
            # yield the descriptor and events
            yield document_name, document
        # make sure the run was finished before it was stopped
        # yield the stop document
        gen = to_stop(scan, start_uid)
        document_name, document = next(gen)
        yield document_name, document


def to_run_start(specscan, **md):
    """Convert a Specscan object into a RunStart document

    Parameters
    ----------
    specscan : suitcase.spec.Specscan
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
    run_start_dict = {
        'time': specscan.time_from_date.timestamp(),
        'scan_id': specscan.scan_id,
        'uid': str(uuid.uuid4()),
        'specpath': specscan.specfile.filename,
        'owner': specscan.specfile.parsed_header['user'],
        'plan_args': specscan.scan_args,
        'motors': [specscan.md['scan_args']['scan_motor']],
        'plan_type': specscan.scan_command,
    }
    run_start_dict.update(**md)
    yield 'start', run_start_dict


def to_baseline(specscan, start_uid):
    """Convert a Specscan object into a baseline Descriptor and Event

    Parameters
    ----------
    specscan : suitcase.spec.Specscan
    start_uid : str
        The uid of the RunStart document that these baseline documents should
        be associated with.

    Yields
    ------
    document_name : str
        'descriptor' followed by 'event'
    baseline_dict : dict
        The baseline EventDescriptor followed by the baseline Event. These
        documents can be inserted into into metadatastore or processed with a
        callback from the ``callbacks`` project.
    """
    timestamp = specscan.time_from_date.timestamp()
    data_keys = {}
    data = {}
    timestamps = {}
    for obj_name, human_name, value in zip(
            specscan.specfile.parsed_header['motor_spec_names'],
            specscan.specfile.parsed_header['motor_human_names'],
            specscan.motor_values):
        data_keys[obj_name] = {'dtype': 'number',
                               'shape': [],
                               'source': human_name}
        data[obj_name] = value
        timestamps[obj_name] = timestamp
    data_keys.update({k: {'dtype': 'number',
                          'shape': [],
                          'source': k} for k in 'hkl'})
    data.update({k: v for k, v in zip('hkl', specscan.hkl)})
    timestamps.update({k: timestamp for k in 'hkl'})
    descriptor = dict(run_start=start_uid, data_keys=data_keys,
                      time=timestamp, uid=str(uuid.uuid4()),
                      name='baseline')
    yield 'descriptor', descriptor
    event = dict(descriptor=descriptor['uid'], seq_num=0, time=timestamp,
                 data=data, timestamps=timestamps, uid=str(uuid.uuid4()))
    yield 'event', event


def to_events(specscan, start_uid):
    """Convert a Specscan object into a Descriptor and Event documents

    Parameters
    ----------
    specscan : suitcase.spec.Specscan
    start_uid : str
        The uid of the RunStart document that these baseline document should be
        associated with.

    Yields
    ------
    document_name : str
        'descriptor' followed by 'event'
    baseline_dict : dict
        The EventDescriptor followed by the stream of Events. These documents
        can be inserted into into metadatastore or processed with a callback
        from the ``callbacks`` project.
    """
    timestamp = specscan.time_from_date.timestamp()
    data_keys = {col: {'dtype': 'number', 'shape': [], 'source': col}
                 for col in specscan.col_names}
    descriptor_uid = dict(run_start=start_uid, data_keys=data_keys,
                          time=timestamp, uid=str(uuid.uuid4()),
                          name='primary')
    yield 'descriptor', descriptor_uid
    timestamps = {col: timestamp for col in specscan.col_names}
    for seq_num, (x, row_series) in enumerate(specscan.scan_data.iterrows()):
        data = {col: data for col, data in zip(row_series.index, row_series[:])}
        event = dict(data=data, descriptor=descriptor_uid,
                     seq_num=seq_num, time=timestamp + data['Epoch'],
                     timestamps=timestamps, uid=str(uuid.uuid4()))
        yield 'event', event


def to_stop(specscan, start_uid, **md):
    """Convert a Specscan object into a Stop document

    Parameters
    ----------
    specscan : suitcase.spec.Specscan
    start_uid : str
        The uid of the RunStart document that this Stop document should
        be associated with.
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
    expected_events = int(specscan.scan_args['strides']) + 1
    if actual_events != expected_events:
        md['reason'] = ('Expected events: {}. Actual events: {}'
                        ''.format(expected_events, actual_events))
        md['exit_status'] = 'abort'
        warnings.warn('scan %s only has %s/%s points. Assuming scan was '
                      'aborted. start_uid=%s' % (specscan.scan_id,
                                                 actual_events,
                                                 expected_events,
                                                 start_uid))
    timestamp = specscan.time_from_date.timestamp()
    stop = dict(run_start=start_uid, time=timestamp, uid=str(uuid.uuid4()),
                **md)
    yield 'stop', stop


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


def to_spec_file_header(start, filepath, baseline_descriptor):
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
    md['positioner_variable_names'] = sorted(list(baseline_descriptor['data_keys'].keys()))
    md['positioner_variable_sources'] = [
        baseline_descriptor['data_keys'][k]['source'] for k
        in md['positioner_variable_names']]
    md['unix_time'] = int(start['time'])
    md['readable_time'] = datetime.fromtimestamp(md['unix_time'])
    md['filename'] = os.path.basename(filepath)
    return _SPEC_FILE_HEADER_TEMPLATE.render(md)


_SPEC_1D_COMMAND_TEMPLATE = env.from_string("{{ plan_type }} {{ scan_motor }} {{ start }} {{ stop }} {{ strides }} {{ time }}")


_PLAN_TO_SPEC_MAPPING = {'AbsScanPlan': 'ascan',
                         'DeltaScanPlan': 'dscan',
                         'Count': 'ct',
                         'Tweak': 'tw'}


_SPEC_SCAN_HEADER_TEMPLATE = env.from_string("""

#S {{ scan_id }} {{ command }}
#D {{ readable_time }}
#T {{ acq_time }} (Seconds)
#P0 {{ positioner_positions | join(' ')}}
#N {{ num_columns }}
#L {{ motor_name }}    Epoch  Seconds  {{ data_keys | join('  ') }}
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
    scan_command = start['plan_type']
    if scan_command in _PLAN_TO_SPEC_MAPPING:
        scan_command = _PLAN_TO_SPEC_MAPPING[start['plan_type']]
    acq_time = _get_acq_time(start)
    md['command'] = ' '.join(
            [scan_command] +
            [start['plan_args'][k]
             for k in ('scan_motor', 'start', 'stop', 'step')] +
            [acq_time])
    md['readable_time'] = to_spec_time(datetime.fromtimestamp(start['time']))
    md['acq_time'] = acq_time
    md['positioner_positions'] = [
        v for k, v in sorted(baseline_event['data_keys'].items())]
    md['num_columns'] = 3 + len(md['data_keys'])
    md['data_keys'] = sorted(list(primary_descriptor['data_keys'].keys()))
    md['motor_name'] = start['plan_args']['scan_motor']
    return _SPEC_SCAN_HEADER_TEMPLATE.render(md)


_SPEC_EVENT_TEMPLATE = env.from_string(
    """{{ motor_position }}  {{ unix_time }} {{ acq_time }} {{ values | join(' ') }}\n""")


def to_spec_scan_data(start, event):
    data = event['data']
    md = {}
    md['unix_time'] = int(event['time'])
    md['acq_time'] = _get_acq_time(start)
    md['motor_position'] =
    pass

class DocumentToSpec(CallbackBase):
    """Callback to export scalar values to a spec file for viewing

    Expect:
        `
    1. a descriptor named 'baseline'
    2. an event for that descriptor
    3. a descriptor named 'main'
    4. events for that descriptor

    Other documents can be issues before, between, and after, but
    these must be issued and in this order.

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
    `Reference <https://github.com/certified-spec/specPy/blob/master/doc/specformat.rst>`_
    for the spec file format.
    """
    def __init__(self, specpath):
        """
        Parameters
        ----------
        specpath : str
            The location on disk where you want the specfile to be written
        """
        self.specpath = specpath
        self.pos_names = ["No", "Positioners", "Were", "Given"]
        self.positions = ["-inf", "-inf", "-inf", "-inf"]

    def _write_spec_header(self, doc):
        """
        Parameters
        ----------
        doc : start document from bluesky

        Returns
        -------
        spec_header : list
            The spec header as a list of lines
        Note
        ----
        Writes a new spec file header that looks like this:
        #F /home/xf11id/specfiles/test.spec
        #E 1449179338.3418093
        #D 2015-12-03 16:48:58.341809
        #C xf11id  User = xf11id
        #O [list of all motors, all on one line]
        """
        content = dict(filepath=self.specpath,
                       unix_time=doc['time'],
                       readable_time=datetime.fromtimestamp(doc['time']),
                       owner=doc['owner'],
                       positioners=self.pos_names)
        with open(self.specpath, 'w') as f:
            f.write(_SPEC_FILE_HEADER_TEMPLATE.render(content))

    def start(self, doc):
        if not os.path.exists(self.specpath):
            spec_header = self._write_spec_header(doc)
        # TODO verify that list of positioners is unchanged  by reading file
        # and parsing any existing contents.
        plan_type = doc['plan_type']
        plan_args = doc['plan_args']
        if plan_type in _PLAN_TO_SPEC_MAPPING.keys():
            # Some of these are used in other methods too -- stash them.
            self._unix_time = doc['time']
            self._acq_time = plan_args.get('time', -1)
            content = dict(plan_type=_PLAN_TO_SPEC_MAPPING[doc['plan_type']],
                           acq_time=self._acq_time)
            if plan_type == 'Count':
                # count has no motor. Have to fake one.
                self._motor = ['Count']
            else:
                self._motor = doc['motors']
                content['start'] = plan_args['start']
                content['stop'] = plan_args['stop']
                content['strides'] = int(plan_args['num']) - 1,
            # We only support a single scanning motor right now.
            if len(self._motor) > 1:
                raise NotImplementedError(
                    "Your scan has %s scanning motors. They are %s. SpecCallback"
                    " cannot handle multiple scanning motors. Please request "
                    "this feature at https://github.com/NSLS-II/suitcase/issues" %
                    (len(self._motor), self._motor))
            self._motor, = self._motor
            content['scan_motor'] = self._motor
            command = _SPEC_1D_COMMAND_TEMPLATE.render(content)
        else:
            err_msg = ("Do not know how to represent %s in SPEC. If "
                       "you would like this feature, request it at "
                       "https://github.com/NSLS-II/bluesky/issues"
                       % plan_type)
            raise NotImplementedError(err_msg)
        # write the new scan entry
        content = dict(command=command,
                       scan_id=doc['scan_id'],
                       readable_time=datetime.fromtimestamp(doc['time']),
                       acq_time=self._acq_time,
                       positioner_positions=self.positions)
        self._start_content = content  # can't write until after we see desc
        self._start_doc = doc

    def descriptor(self, doc):
        """Write the header for the actual scan data"""
        # List all scalar fields, excluding the motor (x variable).
        self._read_fields = sorted([k for k, v in doc['data_keys'].items()
                                    if (v['object_name'] != self._motor
                                        and not v['shape'])])
        content = dict(motor_name=self._motor,
                       acq_time=self._acq_time,
                       unix_time=self._unix_time,
                       length=3 + len(self._read_fields),
                       data_keys=self._read_fields)
        with open(self.specpath, 'a') as f:
            f.write(_SPEC_SCAN_HEADER_TEMPLATE.render(self._start_content))
            f.write(_SPEC_DESCRIPTOR_TEMPLATE.render(content))
            f.write('\n')

    def event(self, doc):
        """Write each event out"""""
        data = doc['data']
        values = [str(data[k]) for k in self._read_fields]
        if self._motor == "Count":
            doc['data']['Count'] = -1
        content = dict(acq_time=self._acq_time,
                       unix_time=doc['time'],
                       motor_position=data[self._motor],
                       values=values)
        with open(self.specpath, 'a') as f:
            f.write(_SPEC_EVENT_TEMPLATE.render(content))
            f.write('\n')

#     def future_descriptor(self, doc):
#         """Write the header for the actual scan data
#         """
#         if 'name' not in doc:
#             return
#         if doc['name'] == 'baseline':
#             self._baseline_desc_uid = doc['uid']
#             # Now we know all the positioners involved and can write the
#             # spec file.
#             pos_names = sorted([dk['object_name'] for dk in doc['data_keys']])
#             self._write_spec_header(self._start_doc, pos_names)
#             with open(self.specpath, 'a') as f:
#                 f.write(_SPEC_SCAN_TEMPLATE.render(content))
#         if doc['name'] == 'main':
#             self._main_desc_uid = doc['main']
#             self._read_fields = sorted([k for k, v in doc['data_keys'].items()
#                                         if v['object_name'] != self._motor])
#         content = dict(motor_name=self._motor,
#                        acq_time=self._acq_time,
#                        unix_time=self._unix_time,
#                        length=3 + len(self._read_fields))
#         with open(self.specpath, 'a') as f:
#             f.write(_SPEC_SCAN_TEMPLATE.render(content))

#     def future_event(self, doc):
#         """
#         Two cases:
#         1. We have a 'baseline' event; write baseline motor positioners
#            and detector values.
#         2. We have a 'main' event; write one line of data.
#         """
#         data = doc['data']
#         if doc['descriptor'] == self._baseline_desc_uid:
#             # This is a 'baseline' event.
#             if self._wrote_baseline_values:
#                 return
#             baseline = {k: str(data[v]) for k, v in sorted(data.items())}
#             with open(self.specpath, 'a') as f:
#                 # using fmt strings; this operation would be a pain with jinja
#                 for idx, (key, val) in enumerate(baseline):
#                     f.write('#M%s %s %s\n' % (idx, key, val))
#             self._wrote_baseline_values = True
#         elif doc['descriptor'] == self._main_desc_uid:
#             values = [str(data[v]) for k, v in self._read_fields]
#             content = dict(acq_time=self._acq_time,
#                            unix_time=self._unix_time,
#                            motor_name=self._motor,
#                            values=values)
#             with open(self.specpath, 'a') as f:
#                 f.write(_SPEC_EVENT_TEMPLATE.render(content))
