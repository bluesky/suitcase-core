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

# need callback base from bluesky
try:
    from bluesky.callbacks import CallbackBase
except ImportError as ie:
    warnings.warn("bluesky.callbacks package is required for some spec "
                  "functionality.")


# Dictionary that maps a spec metadata line to a specific lambda function
# to parse it. This only works for lines whose contents can be mapped to a
# single semantic meaning.  e.g., the "spec command" line
# (ascan start stop step exposure_time) does not map well on to this "single
# semantic meaning" splitter
spec_line_parser = {
    '#D': ('time_from_date',
           lambda x: datetime.strptime(x, '%a %b %d %H:%M:%S %Y')),
    '#E': ('time',
           lambda x: datetime.fromtimestamp(int(x))),
    '#F': ('date',
           lambda x: datetime.strptime(x, '%Y%m%d')),
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
    S_row = raw_scan_data.pop(0).split()
    md['scan_id'] = int(S_row.pop(0))
    md['scan_command'] = S_row.pop(0)
    md['scan_args'] = S_row
    md['motor_values'] = []
    md['geometry'] = []
    line_hash_mapping = {
        '#G': 'geometry',
        '#P': 'motor_values',
    }
    for line in raw_scan_data:
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
        scan_data = [section.split('\n') for section in scan_data]
        self.header = scan_data.pop(0)
        # parse header
        self.parsed_header = parse_spec_header(self.header)
        self.scans = {}
        for scan in scan_data:
            sid = int(scan[0].split()[0])
            self.scans[sid] = Specscan(self, scan)

    def __getitem__(self, key):
        return self.scans[key]

    def __len__(self):
        return len(self.scans)

    def __iter__(self):
        return (self.scans[sid] for sid in sorted(self.scans.keys()))

    def __repr__(self):
        return "Specfile('{}')".format(self.filename)

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
        return obj.specfile == self.specfile and obj.scan_id == self.scan_id

    def __eq__(self, obj):
        return obj.specfile != self.specfile or obj.scan_id != self.scan_id

    def __str__(self):
        return """Scan {}
{}
{} points in the scan
{} """.format(self.scan_id, self.scan_command + " " + " ".join(self.scan_args),
              len(self), self.time_from_date)


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
        document_name, document = next(run_start(scan))
        start_uid = document['uid']
        # yield the start document
        yield document_name, document
        # yield the baseline descriptor and its event
        for document_name, document in baseline(scan, start_uid):
            yield document_name, document
        num_events = 0
        for document_name, document in events(scan, start_uid):
            if document_name == 'event':
                num_events += 1
            # yield the descriptor and events
            yield document_name, document
        # make sure the run was finished before it was stopped
        reason = 'success'
        if num_events != scan.num_points:
            reason = 'abort'
            warnings.warn('scan %s only has %s/%s points. Assuming scan was '
                          'aborted. start_uid=%s' % (scan.scan_id,
                                                     len(num_events),
                                                     scan.num_points,
                                                     start_uid))
        # yield the stop document
        yield stop(scan, start_uid, reason=reason)


def run_start(specscan, **md):
    run_start_dict = {
        'time': specscan.time_from_date.timestamp(),
        'scan_id': specscan.scan_id,
        'uid': str(uuid.uuid4()),
        'specpath': specscan.specfile.filename,
        'owner': specscan.specfile.parsed_header['user'],
        'plan_args': specscan.scan_args,
        'scan_type': specscan.scan_command,
    }
    run_start_dict.update(**md)
    yield 'run_start', run_start_dict


def baseline(specscan, start_uid):
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
    event = dict(descriptor=descriptor_uid, seq_num=0, time=timestamp,
                 data=data, timestamps=timestamps, uid=str(uuid.uuid4()))
    yield 'event', event


def events(specscan, start_uid):
    timestamp = specscan.time_from_date.timestamp()
    data_keys = {}
    data = {}
    timestamps = {}

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


def stop(specscan, start_uid, **md):
    timestamp = specscan.time_from_date.timestamp()
    stop = dict(run_start=start_uid, time=timestamp, uid=str(uuid.uuid4()),
                **md)
    yield 'stop', stop
