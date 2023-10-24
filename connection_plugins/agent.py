from __future__ import (absolute_import, division, print_function)

from io import BytesIO
import requests, json
__metaclass__ = type

import ansible.constants as C
from ansible.errors import AnsibleError, AnsibleConnectionFailure
from ansible.module_utils.six import text_type, binary_type
from ansible.module_utils._text import to_bytes, to_native, to_text
from ansible.plugins.connection import ConnectionBase
from ansible.utils.display import Display

display = Display()

class Connection(ConnectionBase):
    ''' Local based connections '''

    transport = 'local'
    has_pipelining = True

    def __init__(self, *args, **kwargs):
        super(Connection, self).__init__(*args, **kwargs)
        self.cwd = None

    def _connect(self):
        ''' connect to the local host; nothing to do here '''
        return self

    def exec_command(self, cmd, in_data=None, sudoable=True):
        addr = self._play_context.remote_addr
        if isinstance(cmd, (text_type, binary_type)):
            cmd = to_bytes(cmd)
        else:
            cmd = map(to_bytes, cmd)

        files = {}
        if in_data:
            files['stdin'] = BytesIO(in_data)

        resp = requests.post('http://{}:8700/exec'.format(addr), data={'command': cmd}, files=files)
        if not resp.ok:
            raise AnsibleConnectionFailure('Failed to exec command on {}: {}'.format(addr, resp.reason))

        data = resp.json()
        return data['status'], data['stdout'], data['stderr']

    def put_file(self, in_path, out_path):
        with open(in_path, 'rb') as fp:
            remote_addr = self._play_context.remote_addr
            resp = requests.put('http://{}:8700/upload'.format(remote_addr), data={'dest': out_path}, files={'src': fp})

            if not resp.ok:
                raise AnsibleConnectionFailure('Failed to upload file: {}'.format(resp.reason))

    def fetch_file(self, in_path, out_path):
        ''' fetch a file from local to local -- for compatibility '''
        super(Connection, self).fetch_file(in_path, out_path)

        display.vvv(u"FETCH {0} TO {1}".format(in_path, out_path), host=self._play_context.remote_addr)
        self.put_file(in_path, out_path)

    def close(self):
        ''' terminate the connection; nothing to do here '''
        self._connected = False

    def fetch_file(self, in_path, out_path):
        raise AnsibleError("not unimplemented")

    def close(self):
        pass
