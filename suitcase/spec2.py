
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

_SPEC_SCAN_NAMES = ['ascan', 'dscan', 'ct', 'tw']
_BLUESKY_PLAN_NAMES = ['AbsScanPlan', 'DeltaScanPlan', 'Count', 'Tweak']
_PLAN_TO_SPEC_MAPPING = {k: v for k, v in zip(_BLUESKY_PLAN_NAMES,
                                              _SPEC_SCAN_NAMES)}

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

def _get_motor_name(start):
    start['motors']['scan_motor']


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
    md['motor_name'] = _get_motor_name(start)
    return _SPEC_SCAN_HEADER_TEMPLATE.render(md)


_SPEC_EVENT_TEMPLATE = env.from_string(
    """{{ motor_position }}  {{ unix_time }} {{ acq_time }} {{ values | join(' ') }}\n""")


def to_spec_scan_data(start, event):
    data = event['data']
    md = {}
    md['unix_time'] = int(event['time'])
    md['acq_time'] = _get_acq_time(start)
    md['motor_position'] = event.get()
    pass
