# Copyright(C) Facebook, Inc. and its affiliates.
from collections import OrderedDict
from fabric import Connection, ThreadingGroup as Group
from fabric.exceptions import GroupException
from paramiko import RSAKey
from paramiko.ssh_exception import PasswordRequiredException, SSHException
from os.path import basename, splitext
from time import sleep
from math import ceil
from copy import deepcopy
import subprocess

from benchmark.config import Committee, Key, NodeParameters, BenchParameters, ConfigError
from benchmark.utils import BenchError, Print, PathMaker, progress_bar
from benchmark.commands import CommandMaker
from benchmark.logs import LogParser, ParseError
from benchmark.instance import InstanceManager


class FabricError(Exception):
    ''' Wrapper for Fabric exception with a meaningfull error message. '''

    def __init__(self, error):
        assert isinstance(error, GroupException)
        message = list(error.result.values())[-1]
        super().__init__(message)


class ExecutionError(Exception):
    pass


class Bench:
    def __init__(self, ctx):
        self.manager = InstanceManager.make()
        self.settings = self.manager.settings
        '''try:
            ctx.connect_kwargs.pkey = RSAKey.from_private_key_file(
                self.manager.settings.key_path
            )
            self.connect = ctx.connect_kwargs
        except (IOError, PasswordRequiredException, SSHException) as e:
            raise BenchError('Failed to load SSH key', e)'''

    def _check_stderr(self, output):
        if isinstance(output, dict):
            for x in output.values():
                if isinstance(x, str) and x.strip():
                    raise ExecutionError(x.strip())
                elif hasattr(x, 'stderr') and x.stderr:
                    raise ExecutionError(x.stderr.strip())
        else:
            if isinstance(output, str) and output.strip():
                raise ExecutionError(output.strip())
            elif hasattr(output, 'stderr') and output.stderr:
                raise ExecutionError(output.stderr.strip())

    def install(self):
        Print.info('Installing rust and cloning the repo...')
        cmd = [
            'sudo apt-get update',
            'sudo apt-get -y upgrade',
            'sudo apt-get -y autoremove',
            'sudo apt-get -y install build-essential',
            'sudo apt-get -y install cmake',
            'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y',
            'source $HOME/.cargo/env',
            'rustup default nightly',
            'sudo apt-get install -y clang',
            f'cd /home/fiono/ && (git clone {self.settings.repo_url} || (cd {self.settings.repo_name} ; git pull))'
        ]

        # Connect to hosts
        self.manager.connect()

        names = self.manager.names()

        # Execute commands
        for command in cmd:
            for name in names:
                outputs = self.manager.execute_command(name, command)
                #for host, output in outputs:
                    #print(f"Host {host} output:\n{output}")

        Print.heading(f'Initialized testbed of {len(self.manager.hosts())} nodes')

        # Disconnect from hosts
        self.manager.disconnect()

    def kill(self, names, hosts=[], delete_logs=False):
        assert isinstance(hosts, list)
        assert isinstance(delete_logs, bool)
        hosts = hosts if hosts else self.manager.hosts(flat=True)
        delete_logs = CommandMaker.clean_logs() if delete_logs else 'true'
        cmd = [delete_logs, f'({CommandMaker.kill()} || true)']
        try:
            # Execute commands
            for command in cmd:
                for name in names:
                    outputs = self.manager.execute_command(name, command)
                    for host, output in outputs:
                        print(f"Host {host} output:\n{output}")

            Print.heading(f'Initialized testbed of {len(self.manager.hosts())} nodes')

            # Disconnect from hosts
            self.manager.disconnect()
        except GroupException as e:
            raise BenchError('Failed to kill nodes', FabricError(e))

    def _select_hosts(self, bench_parameters):
        return self.manager.hosts()

    def _background_run(self, hosts, command, log_file):
        for host in hosts:
            name = splitext(basename(log_file))[0]
            cmd = f'tmux new -d -s "{name}" "{command} |& tee {log_file}"'
            output = self.manager.execute_command(host, cmd)
            self._check_stderr(output)

    def _update(self, names, hosts, collocate):
        if collocate:
            ips = list(set(hosts))
        else:
            ips = list(set([x for y in hosts for x in y]))

        Print.info(
            f'Updating {len(ips)} machines (branch "{self.settings.branch}")...'
        )
        cmd = [
            f'(cd {self.settings.repo_name} && git fetch -f)',
            f'(cd {self.settings.repo_name} && git checkout -f {self.settings.branch})',
            f'(cd {self.settings.repo_name} && git pull -f)',
            'source $HOME/.cargo/env',
            f'(cd {self.settings.repo_name}/node && {CommandMaker.compile()})',
            CommandMaker.alias_binaries(
                f'./{self.settings.repo_name}/target/release/'
            )
        ]

        # Execute commands on each IP
        for name in names:
            for command in cmd:
                output = self.manager.execute_command(name, command)
                print(f"Host {name} output:\n{output}")

    def _config(self, hosts_names, hosts, node_parameters, bench_parameters):
        Print.info('Generating configuration files...')

        for host_name in hosts_names:
            # Cleanup all local configuration files.
            cmd = CommandMaker.cleanup()
            self.manager.execute_command(host_name, cmd)

            # Recompile the latest code.
            cmd = CommandMaker.compile()
            self.manager.execute_command(host_name, cmd, check=True, cwd=PathMaker.node_crate_path())

            # Create alias for the client and nodes binary.
            cmd = CommandMaker.alias_binaries(PathMaker.binary_path())
            self.manager.execute_command(host_name, cmd)
            #self.manager.execute_command(host_name, f'./node generate_keys --filename .node-0.json')

            # Generate configuration files.
            keys = []
            key_files = [PathMaker.key_file(i) for i in range(len(hosts))]
            for filename in key_files:
                cmd = CommandMaker.generate_key(filename)
                self.manager.execute_command(host_name, cmd, check=False)
                keys += [Key.from_file(filename)]

        names = [x.name for x in keys]

        if bench_parameters.collocate:
            workers = bench_parameters.workers
            addresses = OrderedDict(
                (x, [y] * (workers + 1)) for x, y in zip(names, hosts)
            )
        else:
            addresses = OrderedDict(
                (x, y) for x, y in zip(names, hosts)
            )
        committee = Committee(addresses, self.settings.base_port)
        cmd = CommandMaker.make_committee(committee, PathMaker.committee_file())
        #committee.print(PathMaker.committee_file())

        for host_name in hosts_names:
            self.manager.execute_command(host_name, cmd)

        node_parameters.print(PathMaker.parameters_file())

        # Cleanup all nodes and upload configuration files.
        names = names[:len(names)-bench_parameters.faults]
        progress = progress_bar(names, prefix='Uploading config files:')
        for i, name in enumerate(progress):
            for ip in hosts_names:
                command = f'{CommandMaker.cleanup()} || true'
                self.manager.execute_command(ip, command)
                self.manager.execute_command(ip, f'put {PathMaker.committee_file()} .')
                self.manager.execute_command(ip, f'put {PathMaker.key_file(i)} .')
                self.manager.execute_command(ip, f'put {PathMaker.parameters_file()} .')

        return committee

    def _run_single(self, names, rate, committee, bench_parameters, debug=False):
        faults = bench_parameters.faults

        # Kill any potentially unfinished run and delete logs.
        hosts = committee.ips()
        #self.kill(names, hosts=hosts, delete_logs=True)

        # Run the clients (they will wait for the nodes to be ready).
        # Filter all faulty nodes from the client addresses (or they will wait
        # for the faulty nodes to be online).
        Print.info('Booting clients...')
        workers_addresses = committee.workers_addresses(faults)
        rate_share = ceil(rate / committee.workers())
        for i, addresses in enumerate(workers_addresses):
            for (id, address) in addresses:
                host = Committee.ip(address)
                cmd = CommandMaker.run_client(
                    address,
                    bench_parameters.tx_size,
                    rate_share,
                    [x for y in workers_addresses for _, x in y]
                )
                log_file = PathMaker.client_log_file(i, id)
                self._background_run(names, cmd, log_file)

        # Run the primaries (except the faulty ones).
        Print.info('Booting primaries...')
        for i, address in enumerate(committee.primary_addresses(faults)):
            host = Committee.ip(address)
            cmd = CommandMaker.run_primary(
                PathMaker.key_file(i),
                PathMaker.committee_file(),
                PathMaker.db_path(i),
                PathMaker.parameters_file(),
                debug=debug
            )
            log_file = PathMaker.primary_log_file(i)
            self._background_run(names, cmd, log_file)

        # Run the workers (except the faulty ones).
        Print.info('Booting workers...')
        for i, addresses in enumerate(workers_addresses):
            for (id, address) in addresses:
                host = Committee.ip(address)
                cmd = CommandMaker.run_worker(
                    PathMaker.key_file(i),
                    PathMaker.committee_file(),
                    PathMaker.db_path(i, id),
                    PathMaker.parameters_file(),
                    id,  # The worker's id.
                    debug=debug
                )
                log_file = PathMaker.worker_log_file(i, id)
                self._background_run(names, cmd, log_file)

        # Wait for all transactions to be processed.
        duration = bench_parameters.duration
        for _ in progress_bar(range(20), prefix=f'Running benchmark ({duration} sec):'):
            sleep(ceil(duration / 20))
        #self.kill(names, hosts=hosts, delete_logs=False)

    def _logs(self, names, committee, faults):
        for name in names:
            # Delete local logs (if any).
            cmd = CommandMaker.clean_logs()
            self.manager.execute_command(name, cmd)

            # Download log files.
            workers_addresses = committee.workers_addresses(faults)
            progress = progress_bar(workers_addresses, prefix='Downloading workers logs:')
            for i, addresses in enumerate(progress):
                for id, address in addresses:
                    #host = Committee.ip(address)
                    host = names[i]
                    output = self.manager.execute_command(host, f'cat {PathMaker.client_log_file(i, id)}')
                    with open(PathMaker.client_log_file(i, id), 'w') as file:
                        file.write(output)
                    output = self.manager.execute_command(host, f'cat {PathMaker.worker_log_file(i, id)}')
                    with open(PathMaker.worker_log_file(i, id), 'w') as file:
                        file.write(output)

            primary_addresses = committee.primary_addresses(faults)
            progress = progress_bar(primary_addresses, prefix='Downloading primaries logs:')
            for i, address in enumerate(progress):
                #host = Committee.ip(address)
                host = names[i]
                output = self.manager.execute_command(host, f'cat {PathMaker.primary_log_file(i)}')
                with open(PathMaker.primary_log_file(i), 'w') as file:
                    file.write(output)

        # Parse logs and return the parser.
        Print.info('Parsing logs and computing performance...')
        return LogParser.process(PathMaker.logs_path(), faults=faults)

    def run(self, bench_parameters_dict, node_parameters_dict, debug=False):
        assert isinstance(debug, bool)
        Print.heading('Starting remote benchmark')
        try:
            bench_parameters = BenchParameters(bench_parameters_dict)
            node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)
        
        # Connect to hosts
        self.manager.connect()

        cmd = f'cd {self.settings.repo_name}/benchmark'
        names = self.manager.names()

        for name in names:
            self.manager.execute_command(name, cmd)

        # Select which hosts to use.
        selected_hosts = self._select_hosts(bench_parameters)
        if not selected_hosts:
            Print.warn('There are not enough instances available')
            return

        # Update nodes.
        try:
            self._update(names, selected_hosts, bench_parameters.collocate)
        except (GroupException, ExecutionError) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to update nodes', e)

        # Upload all configuration files.
        try:
            committee = self._config(
                names, selected_hosts, node_parameters, bench_parameters
            )
        except (subprocess.SubprocessError, GroupException) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to configure nodes', e)
        
        # Run benchmarks.
        for n in bench_parameters.nodes:
            committee_copy = deepcopy(committee)
            committee_copy.remove_nodes(committee.size() - n)

            for r in bench_parameters.rate:
                Print.heading(f'\nRunning {n} nodes (input rate: {r:,} tx/s)')

                # Run the benchmark.
                for i in range(bench_parameters.runs):
                    Print.heading(f'Run {i+1}/{bench_parameters.runs}')
                    try:
                        self._run_single(
                            names, r, committee_copy, bench_parameters, debug
                        )

                        faults = bench_parameters.faults
                        logger = self._logs(names, committee_copy, faults)
                        logger.print(PathMaker.result_file(
                            faults,
                            n, 
                            bench_parameters.workers,
                            bench_parameters.collocate,
                            r, 
                            bench_parameters.tx_size, 
                        ))
                    except (subprocess.SubprocessError, GroupException, ParseError) as e:
                        self.kill(names, hosts=selected_hosts)
                        if isinstance(e, GroupException):
                            e = FabricError(e)
                        Print.error(BenchError('Benchmark failed', e))
                        continue
            #self.kill(names, hosts=selected_hosts)
