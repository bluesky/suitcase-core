# From T. Caswell
# ingest Athena docs into databroker (for Materials Project)
import pandas as pd
import uuid
import time

def _parse_athena_header(inp_lines):
    out = {}
    for ln in inp_lines:
        key, sep, val = ln[2:].partition(':')
        if not sep:
            continue
        key = key.strip()
        val = val.strip()
        if not key or not val:
            continue
        target = out
        split_key = key.split('.')
        for k in split_key[:-1]:
            target.setdefault(k, {})
            target = target[k]
        target[split_key[-1]] = val

    return out

def parse_athena_file(fname):
    _magic_first_string = '# XDI/1.0  Athena/0.9.25'
    _magic_end_string = '# ///'
    with open(fname, 'r') as fin:
        ln = next(fin)
        if ln.strip() != _magic_first_string:
            raise TypeError("magic string wrong, expected {_magic_first_string!r} "
                            "but got {ln!r}")

        hdr_lines = []
        for ln in fin:
            ln = ln.strip()
            if ln == _magic_end_string:
                break
            hdr_lines.append(ln)

        md = _parse_athena_header(hdr_lines)
        # jump past the divider
        next(fin)
        cols = next(fin).strip('#').split()
        payload = pd.read_csv(fin, sep='\s+', names=cols).set_index('e')

    return md, payload


def ingest(fname, md=None):
    ''' Ingest Athena file.

        This file has a header and then some data in row, col ascii format.

        Parameters
        ----------
        fname : str
            the filename to read from (Athena file)

        md : dict, optional
            additional metadata to add (author name? etc)

        Returns
        -------
            Generator of documents to be inserted into databroker

    '''
    new_md, payload = parse_athena_file(fname)
    payload = payload.reset_index()
    doc_time = time.time()
    new_md.update(md)
    start = {'uid': str(uuid.uuid4()),
             'time': doc_time,
             **new_md}

    yield 'start', start

    desc = {'uid': str(uuid.uuid4()),
            'time': doc_time,
            'run_start': start['uid'],
            'name': 'primary',
            'data_keys': {k: {'dtype': 'number', 'shape': None, 'source':
'Athena'}
                          for k in payload.columns}}

    yield 'descriptor', desc
    _ts = {k: doc_time for k in payload.columns}
    for sq, row in payload.iterrows():
        yield 'event', {'uid': str(uuid.uuid4()),
                        'descriptor': desc['uid'],
                        'time': doc_time,
                        'seq_num': sq + 1,
                        'data': dict(row),
                        'filled': {},
                        'timestamps': _ts}

    yield 'stop', {'uid': str(uuid.uuid4()),
                   'time': doc_time,
                   'run_start': start['uid'],
                   'exit_status': 'success',
                   'num_events': {'primary': len(payload)}}
