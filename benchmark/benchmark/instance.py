import subprocess
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
                    print(f"Connecting to {host['host']}...")
                    client.connect(
                        host['host'], 
                        port=host['port'], 
                        username=host['username'], 
                        password=host['password']
                    )
                    print(f"Connected to {host['host']}")
                except paramiko.AuthenticationException:
                    raise SSHError(f"Authentication failed for {name}, please verify your credentials")

    def disconnect(self):
        for client in self.clients.values():
            client.close()
        print("Disconnected from all hosts.")

    def execute_command(self, ip, command, check=False, cwd=None):
        print("ip: ", ip)
        print("command: ", command)
        if ip == self.settings.localhost:
            # Run the command locally
            output = subprocess.run(command, shell=True, check=check, capture_output=True, text=True, cwd=cwd)
            return output.stdout
        else:
            # Run the command on the SSH instance
            client = self.clients.get(ip)
            if client:
                stdin, stdout, stderr = client.exec_command(command)
                return stdout.read().decode()
            else:
                raise SSHError(f"No client found for IP: {ip}")

    def hosts(self):
        ssh_hosts = [host['host'] for host in self.settings.ssh_details]
        return [self.settings.localhost] + ssh_hosts
    
    def names(self):
        ssh_names = [name['name'] for name in self.settings.ssh_details]
        return [self.settings.localhost] + ssh_names

    def print_info(self):
        for name, client in self.clients.items():
            host = next((host for host in self.settings.ssh_details if host["name"] == name), None)
            if host:
                print(f"\nConnected to host: {host['host']}")
                print(f"Username: {host['username']}")
                print(f"Port: {host['port']}\n")
