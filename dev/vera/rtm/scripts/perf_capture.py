"""Query-scoped user-space profiling of the benchmark's own executor PIDs."""
import json
import os
import pathlib
import select
import signal
import subprocess
import time


class PerfCapture:
    def __init__(self, out, query, executors):
        self.directory = pathlib.Path(out) / 'perf' / query
        self.directory.mkdir(parents=True, exist_ok=False)
        self.all_executors = executors
        # Balance samples across NUMA nodes without creating thousands of mmap buffers.
        self.executors = [x for node in ('bind:0', 'bind:1')
                          for x in sorted((e for e in executors if e['numa_policy'] == node),
                                          key=lambda e: e['pid'])[:4]]
        self.children = []
        self.stopped = False

    def _command(self, child, command):
        os.write(child['control'], (command+'\n').encode())
        deadline = time.monotonic()+60
        data = b''
        while b'ack\n' not in data:
            if child['process'].poll() is not None:
                raise RuntimeError(f"perf {child['kind']} exited; see {self.directory}")
            if time.monotonic() > deadline:
                raise RuntimeError(f"perf {child['kind']} acknowledgement timeout")
            if select.select([child['ack']], [], [], 0.2)[0]:
                part = os.read(child['ack'], 1024)
                if not part:
                    raise RuntimeError('perf closed acknowledgement pipe')
                data += part

    def start(self):
        pids = ','.join(str(x['pid']) for x in self.executors)
        assert len(self.all_executors) == 42 and len(self.executors) == 8
        commands = {
            'record': ['perf', 'record', '-p', pids, '-e', 'armv8_pmuv3_0/cpu_cycles/u', '-F', '99',
                       '--per-thread', '-m', '1', '-o', str(self.directory/'cycles.data'), '-D', '-1'],
            'stat': ['perf', 'stat', '-p', pids, '-e',
                     'armv8_pmuv3_0/cpu_cycles/u,armv8_pmuv3_0/inst_retired/u,'
                     'armv8_pmuv3_0/l1d_cache_refill/u,'
                     'armv8_pmuv3_0/l2d_cache_refill/u,'
                     'armv8_pmuv3_0/stall_backend_mem/u',
                     '-x', ';', '-o', str(self.directory/'counters.csv'), '-D', '-1'],
        }
        self.thread_snapshot = {}
        for executor in self.executors:
            for task in pathlib.Path(f"/proc/{executor['pid']}/task").iterdir():
                try:
                    self.thread_snapshot[task.name] = (task/'comm').read_text().strip()
                except FileNotFoundError:
                    pass
        (self.directory/'capture.json').write_text(json.dumps({
            'executors': self.executors, 'registered_executor_count': len(self.all_executors), 'commands': commands,
            'scope': 'User-space CPU on eight warmed executor PIDs, four per NUMA node; existing threads only, flat instruction-pointer samples, no call stacks.',
            'timing_eligible': False,
            'threads_at_attach': self.thread_snapshot,
            'inherit': False,
        }, indent=2)+'\n')
        try:
            for kind, args in commands.items():
                control_read, control_write = os.pipe()
                ack_read, ack_write = os.pipe()
                log = (self.directory/(kind+'.log')).open('w')
                command = args+['--control', f'fd:{control_read},{ack_write}']
                try:
                    process = subprocess.Popen(command, pass_fds=(control_read, ack_write),
                                               stdout=log, stderr=subprocess.STDOUT)
                except BaseException:
                    os.close(control_write)
                    os.close(ack_read)
                    log.close()
                    raise
                finally:
                    os.close(control_read)
                    os.close(ack_write)
                self.children.append({'kind': kind, 'process': process, 'control': control_write,
                                      'ack': ack_read, 'log': log})
            for child in self.children:
                self._command(child, 'enable')
            self.started = time.time()
        except BaseException:
            self.stop(disable=False)
            raise

    def stop(self, disable=True):
        if self.stopped:
            return
        self.stopped = True
        errors = []
        records = []
        for child in self.children:
            process = child['process']
            if disable and process.poll() is None:
                try:
                    self._command(child, 'disable')
                except Exception as exc:
                    errors.append(str(exc))
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
            try:
                process.wait(timeout=60)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
                errors.append(f"perf {child['kind']} flush timeout")
            os.close(child['control'])
            os.close(child['ack'])
            child['log'].close()
            records.append({'kind': child['kind'], 'exit_code': process.returncode})
            if process.returncode not in (0, 130, -signal.SIGINT):
                errors.append(f"perf {child['kind']} exited {process.returncode}")
        new_threads = {}
        for executor in self.executors:
            try:
                for task in pathlib.Path(f"/proc/{executor['pid']}/task").iterdir():
                    if task.name not in self.thread_snapshot:
                        try:
                            new_threads[task.name] = (task/'comm').read_text().strip()
                        except FileNotFoundError:
                            pass
            except FileNotFoundError:
                pass
        (self.directory/'completion.json').write_text(json.dumps({
            'started': getattr(self, 'started', None), 'ended': time.time(),
            'processes': records, 'errors': errors,
            'new_threads_not_sampled': new_threads,
        }, indent=2)+'\n')
        if errors and disable:
            raise RuntimeError('; '.join(errors))
