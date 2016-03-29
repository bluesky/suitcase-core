"""
`Reference <https://github.com/certified-spec/specPy/blob/master/doc/specformat.rst>`_
for the spec file format.
"""
import numpy as np
import pandas as pd
import os
from datetime import datetime

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
            print("I am not sure how to parse %s" % line_type)
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
