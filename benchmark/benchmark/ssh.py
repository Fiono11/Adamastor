import subprocess
from collections import OrderedDict

class SSHError(Exception):
    def __init__(self, error):
        self.message = str(error)
        super().__init__(self.message)

class SSHInstanceManager:

    def __init__(self, hosts):
        # Hosts would be a list of tuples with (username, hostname)
        assert isinstance(hosts, list)
        self.hosts = hosts

    @staticmethod
    def run_command(host, command):
        username, hostname = host
        try:
            result = subprocess.run(['ssh', f'{username}@{hostname}', command], capture_output=True, check=True)
            return result.stdout
        except subprocess.CalledProcessError as e:
            raise SSHError(e)

    def run_command_on_all(self, command):
        results = OrderedDict()
        for host in self.hosts:
            results[host] = self.run_command(host, command)
        return results
