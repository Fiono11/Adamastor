# Copyright(C) Facebook, Inc. and its affiliates.
from datetime import datetime
from glob import glob
import io
from multiprocessing import Pool
from os.path import join
from re import findall, search
from statistics import mean
import re

from benchmark.utils import Print


class ParseError(Exception):
    pass


class LogParser:
    def __init__(self, clients, primaries, workers, faults, directory):
        inputs = [clients, primaries, workers]
        assert all(isinstance(x, list) for x in inputs)
        assert all(isinstance(x, str) for y in inputs for x in y)
        assert all(x for x in inputs)

        self.faults = faults
        if isinstance(faults, int):
            self.committee_size = len(primaries) + int(faults)
            self.workers =  len(workers) // len(primaries)
        else:
            self.committee_size = '?'
            self.workers = '?'

        log_files = ['logs/client-0-0.log', 'logs/client-1-0.log', 'logs/client-2-0.log', 'logs/client-3-0.log', 'logs/client-4-0.log', 'logs/client-5-0.log', 'logs/client-6-0.log']  # replace these with your actual log file paths
        self.parse_logs(log_files)

        clients = []
        for filename in sorted(glob(join(directory, 'client-*-*'))):
            #if num < correct:
            with open(filename, 'r') as f:
                clients += [f.read()]

        # Parse the clients logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_clients, clients)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse clients\' logs: {e}')
        self.size, self.rate, self.start, misses, self.sent_samples \
            = zip(*results)
        self.misses = sum(misses)

        # Parse the primaries logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_primaries, primaries)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse nodes\' logs: {e}')
        proposals, commits, self.configs, primary_ips = zip(*results)
        self.proposals = self._merge_results([x.items() for x in proposals])
        self.commits = self._merge_results([x.items() for x in commits])
        #print("self commits: ", self.commits)

        # Parse the workers logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_workers, workers)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse workers\' logs: {e}')
        sizes, self.received_samples, workers_ips = zip(*results)
        #print("comits: ", self.commits)
        self.sizes = {
            k: v for x in sizes for k, v in x.items() if k in self.commits
        }

        #print("sizes: ", self.sizes)

        # Determine whether the primary and the workers are collocated.
        self.collocate = set(primary_ips) == set(workers_ips)

        # Check whether clients missed their target rate.
        if self.misses != 0:
            Print.warn(
                f'Clients missed their target rate {self.misses:,} time(s)'
            )

    def _merge_results(self, input):
        # Keep the earliest timestamp.
        merged = {}
        for x in input:
            for k, v in x:
                if not k in merged or merged[k] > v:
                    merged[k] = v
        return merged
    
    def _merge_results2(self, input):
        # Initialize sum of keys and minimum value.
        sum_keys = 0
        min_value = None
        for x in input:
            for k, v in x:
                # Add key to sum.
                sum_keys += int(k)
                # Keep the smallest value.
                if min_value is None or v < min_value:
                    min_value = v
        return {sum_keys: min_value}
    
    def parse_logs(self, log_files):
        self.trans_id = 0
        num_clients = len(log_files)
        client_logs = [None] * num_clients
        for idx, file in enumerate(log_files):
            # Read the file
            with open(file, 'r') as f:
                client_logs[idx] = f.readlines()

        max_trans = max(len(log) for log in client_logs)

        # Initialize new_logs with deep copy of client_logs
        new_logs = [list(log) for log in client_logs]

        for trans in range(max_trans):
            for client in range(num_clients):
                if trans < len(client_logs[client]) and trans > 6:
                    #if trans < 50:
                        #print("replacing ", client_logs[client][trans], " by ", self.trans_id)
                    # Replace the line with the new transaction ID
                    new_logs[client][trans] = re.sub(r'Sending sample transaction \d+', f'Sending sample transaction {self.trans_id}', client_logs[client][trans])
                    # Increment the global transaction ID
                    self.trans_id += 1

        # Write the new content back into the files
        for idx, file in enumerate(log_files):
            with open(file, 'w') as f:
                f.writelines(new_logs[idx])

    def _parse_clients(self, log):
        if search(r'Error', log) is not None:
            raise ParseError('Client(s) panicked')

        size = int(search(r'Transactions size: (\d+)', log).group(1))
        rate = int(search(r'Transactions rate: (\d+)', log).group(1))

        tmp = search(r'\[(.*Z) .* Start ', log).group(1)
        start = self._to_posix(tmp)

        misses = len(findall(r'rate too high', log))

        tmp = findall(r'\[(.*Z) .* sample transaction (\d+)', log)
        #print("tmp: ", tmp)
        samples = {int(s): self._to_posix(t) for t, s in tmp}
        #print("samples: ", samples)

        return size, rate, start, misses, samples

    def _parse_primaries(self, log):
        if search(r'(?:panicked|Error)', log) is not None:
            raise ParseError('Primary(s) panicked')

        tmp = findall(r'\[(.*Z) .* Created B\d+\([^ ]+\) -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        proposals = self._merge_results([tmp])

        tmp = findall(r'\[(.*Z) .* Committed (\d+) -> (\d+)', log)
        #print("tmp1: ", tmp)
        tmp = [(a, (d, self._to_posix(t))) for t, d, a in tmp]
        #print("tmp2: ", tmp)
        commits = self._merge_results([tmp])
        #print("commits: ", commits)

        configs = {
            'header_size': int(
                search(r'Header size .* (\d+)', log).group(1)
            ),
            'max_header_delay': int(
                search(r'Max header delay .* (\d+)', log).group(1)
            ),
            'gc_depth': int(
                search(r'Garbage collection depth .* (\d+)', log).group(1)
            ),
            'sync_retry_delay': int(
                search(r'Sync retry delay .* (\d+)', log).group(1)
            ),
            'sync_retry_nodes': int(
                search(r'Sync retry nodes .* (\d+)', log).group(1)
            ),
            'batch_size': int(
                search(r'Batch size .* (\d+)', log).group(1)
            ),
            'max_batch_delay': int(
                search(r'Max batch delay .* (\d+)', log).group(1)
            ),
        }

        ip = search(r'booted on (\d+.\d+.\d+.\d+)', log).group(1)
        
        return proposals, commits, configs, ip

    def _parse_workers(self, log):
        if search(r'(?:panic|Error)', log) is not None:
            raise ParseError('Worker(s) panicked')

        tmp = findall(r'Batch ([^ ]+) contains (\d+) B', log)
        sizes = {d: int(s) for d, s in tmp}

        #print("sizes: ", sizes.values())

        tmp = findall(r'Batch ([^ ]+) contains sample tx (\d+)', log)
        samples = {int(s): d for d, s in tmp}

        ip = search(r'booted on (\d+.\d+.\d+.\d+)', log).group(1)

        return sizes, samples, ip

    def _to_posix(self, string):
        x = datetime.fromisoformat(string.replace('Z', '+00:00'))
        return datetime.timestamp(x)

    '''def _consensus_throughput(self):
        if not self.commits:
            return 0, 0, 0
        start, end = min(self.proposals.values()), max(self.commits.values())
        duration = end - start
        bytes = sum(self.sizes.values())
        #print("proposals: ", self.proposals)
        #print("commits: ", self.commits)
        print("bytes: ", bytes)
        print("duration: ", duration)
        bps = bytes / duration
        tps = bps / self.size[0]
        return tps, bps, duration'''

    def _consensus_latency(self):
        #latency = [c - self.proposals[d] for d, c in self.commits.items()]
        #return mean(latency) if latency else 0
        0

    def _end_to_end_throughput(self):
        if not self.commits:
            return 0, 0, 0
        start, end = min(self.start), max(val[1] for val in self.commits.values())
        duration = end - start

        sum_values = sum(int(val[0]) for val in self.commits.values())
        bytes = sum_values * 532  # Convert the key to an integer
        print("bytes: ", bytes)
        #bytes = sum(self.sizes.values())
        bps = bytes / duration
        tps = bps / self.size[0]
        print("tps: ", tps)
        print("duration: ", duration)
        return tps, bps, duration

    def _end_to_end_latency(self):
        #start, end = min(self.start), max(val[1] for val in self.commits.values())
        #print("sent: ", self.sent_samples)
        #print("commits: ", self.commits)
        latency = []
        keys = list(self.commits.keys())
        counter = 0
        merged_dict = {k: v for d in self.sent_samples for k, v in d.items()}
        #print(len(merged_dict))
        #print("self samples: ", self.sent_samples)

        for i in range(len(keys)):
            #print("counter: ", counter)
            start = merged_dict[counter]
            end = self.commits[keys[i]][1]
            latency += [end-start]
            counter += int(self.commits[keys[i]][0])-1

        print("latency: ", mean(latency))
        return mean(latency) if latency else 0

    def result(self):
        header_size = self.configs[0]['header_size']
        max_header_delay = self.configs[0]['max_header_delay']
        gc_depth = self.configs[0]['gc_depth']
        sync_retry_delay = self.configs[0]['sync_retry_delay']
        sync_retry_nodes = self.configs[0]['sync_retry_nodes']
        batch_size = self.configs[0]['batch_size']
        max_batch_delay = self.configs[0]['max_batch_delay']

        consensus_latency = 0#self._consensus_latency() * 1_000
        consensus_tps, consensus_bps, _ = 0, 0, 0#self._consensus_throughput()
        end_to_end_tps, end_to_end_bps, duration = self._end_to_end_throughput()
        end_to_end_latency = self._end_to_end_latency() * 1_000

        return (
            '\n'
            '-----------------------------------------\n'
            ' SUMMARY:\n'
            '-----------------------------------------\n'
            ' + CONFIG:\n'
            f' Faults: {self.faults} node(s)\n'
            f' Committee size: {self.committee_size} node(s)\n'
            f' Worker(s) per node: {self.workers} worker(s)\n'
            f' Collocate primary and workers: {self.collocate}\n'
            f' Input rate: {sum(self.rate):,} tx/s\n'
            f' Transaction size: {self.size[0]:,} B\n'
            f' Execution time: {round(duration):,} s\n'
            '\n'
            f' Header size: {header_size:,} B\n'
            f' Max header delay: {max_header_delay:,} ms\n'
            f' GC depth: {gc_depth:,} round(s)\n'
            f' Sync retry delay: {sync_retry_delay:,} ms\n'
            f' Sync retry nodes: {sync_retry_nodes:,} node(s)\n'
            f' batch size: {batch_size:,} B\n'
            f' Max batch delay: {max_batch_delay:,} ms\n'
            '\n'
            ' + RESULTS:\n'
            f' Consensus TPS: {round(consensus_tps):,} tx/s\n'
            f' Consensus BPS: {round(consensus_bps):,} B/s\n'
            f' Consensus latency: {round(consensus_latency):,} ms\n'
            '\n'
            f' End-to-end TPS: {round(end_to_end_tps):,} tx/s\n'
            f' End-to-end BPS: {round(end_to_end_bps):,} B/s\n'
            f' End-to-end latency: {round(end_to_end_latency):,} ms\n'
            '-----------------------------------------\n'
        )

    def print(self, file):
        if isinstance(file, str):
            with open(file, 'a') as f:
                f.write(self.result())
        elif isinstance(file, io.StringIO):
            file.write(self.result())
        else:
            raise ValueError("Expected a filename or StringIO. Got %s" % type(file))

    @classmethod
    def process(cls, directory, faults):
        assert isinstance(directory, str)

        clients = []
        for filename in sorted(glob(join(directory, 'client-*-*'))):
            num = int(re.search(r'client-(\d+)-\d+.log', filename).group(1))
            #if num < correct:
            with open(filename, 'r') as f:
                clients += [f.read()]
        primaries = []
        for filename in sorted(glob(join(directory, 'primary-*.log'))):
            num = int(re.search(r'primary-(\d+).log', filename).group(1))
            #if num < correct:
            with open(filename, 'r') as f:
                primaries += [f.read()]
        workers = []
        for filename in sorted(glob(join(directory, 'worker-*-*'))):
            num = int(re.search(r'worker-(\d+)-\d+.log', filename).group(1))
            #if num < correct:
            with open(filename, 'r') as f:
                workers += [f.read()]

        return cls(clients, primaries, workers, faults, directory)