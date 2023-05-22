import paramiko
from benchmark.settings import Settings, SettingsError

class SSHError(Exception):
    pass

class InstanceManager:
    def __init__(self, settings):
        assert isinstance(settings, Settings)
        self.settings = settings
        self.clients = {host['name']: paramiko.SSHClient() for host in settings.ssh_details}
        for client in self.clients.values():
            client.load_system_host_keys()
            client.set_missing_host_key_policy(paramiko.WarningPolicy)

    @classmethod
    def make(cls, settings_file='settings.json'):
        try:
            return cls(Settings.load(settings_file))
        except SettingsError as e:
            raise SSHError('Failed to load settings', e)

    def connect(self):
        for name, client in self.clients.items():
            host = next((host for host in self.settings.ssh_details if host["name"] == name), None)
            if host:
                try:
                    client.connect(
                        host['host'], 
                        port=host['port'], 
                        username=host['username'], 
                        password=host['password']
                    )
                except paramiko.AuthenticationException:
                    raise SSHError(f"Authentication failed for {name}, please verify your credentials")

    def disconnect(self):
        for client in self.clients.values():
            client.close()

    def execute_command(self, command):
        outputs = {}
        for name, client in self.clients.items():
            stdin, stdout, stderr = client.exec_command(command)
            outputs[name] = stdout.read().decode()
        return outputs

    def hosts(self):
        return [host['host'] for host in self.settings.ssh_details]

    def print_info(self):
        for name, client in self.clients.items():
            host = next((host for host in self.settings.ssh_details if host["name"] == name), None)
            if host:
                print(f"\nConnected to host: {host['host']}")
                print(f"Username: {host['username']}")
                print(f"Port: {host['port']}\n")
