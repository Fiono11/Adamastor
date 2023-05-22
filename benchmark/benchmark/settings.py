# Copyright(C) Facebook, Inc. and its affiliates.
from json import load, JSONDecodeError

class SettingsError(Exception):
    pass

class Settings:
    def __init__(self, ssh_details, repo_name, repo_url, branch):
        if isinstance(ssh_details, list):
            for ssh_detail in ssh_details:
                if not all(isinstance(ssh_detail.get(key), str) for key in ['name', 'host', 'username', 'password']):
                    raise SettingsError('Invalid settings types')
                if not isinstance(ssh_detail.get('port'), int):
                    raise SettingsError('Invalid settings types')
        else:
            raise SettingsError('ssh_details should be a list')

        self.ssh_details = ssh_details
        self.repo_name = repo_name
        self.repo_url = repo_url
        self.branch = branch

    @classmethod
    def load(cls, filename):
        try:
            with open(filename, 'r') as f:
                data = load(f)

            return cls(
                data['ssh_details'],
                data['repo']['name'],
                data['repo']['url'],
                data['repo']['branch'],
            )
        except (OSError, JSONDecodeError) as e:
            raise SettingsError(str(e))

        except KeyError as e:
            raise SettingsError(f'Malformed settings: missing key {e}')
