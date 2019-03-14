# -*- coding: utf-8 -*-
from supervisor.supervisorctl import ControllerPluginBase
from supervisor import xmlrpc
from supervisor_mightyreload.contrib import LSBInitExitStatuses
from threading import Thread

import xmlrpclib
import json
import fnmatch


class MightyReloadControllerPlugin(ControllerPluginBase):
    name = 'mightyreload'

    def __init__(self, controller, **config):
        self.ctl = controller
        self.supervisor = controller.get_server_proxy('supervisor')
        self.mightyreload = controller.get_server_proxy('mightyreload')
        self.match_group = bool(int(config.get('match_group', '1')))

    # Quick restart and status block on thread
    def _match_process(self, process, pattern):
        name = process['name']
        if self.match_group:
            name = "%s:%s" % (process['group'], process['name'])
        return fnmatch.fnmatch(name, pattern)

    def _expand_wildcards(self, arg, command):
        patterns = arg.split()
        supervisor = self.ctl.get_supervisor()
        if 'all' in patterns:
            # match any process name
            patterns = ['*']

        # Create list for threading restart
        threads = []
        # Create set for monitoring
        procgroupset = set()
        for process in supervisor.getAllProcessInfo():
            for pattern in patterns:
                if self._match_process(process, pattern):
                    # Fill set with all
                    procgroupset.add(process['group'])
                    t = Thread(target=self.ctl.onecmd, args=('%s %s:%s' % (command, process['group'],
                                                                           process['name']), ))
                    t.start()
                    threads.append(t)

        for t in threads:
            t.join()

        if not threads:
            self.ctl.output('No process matched given expression.')

    def _wrap_help(self, command):
        self.ctl.output('The same as %s, but accepts wildcard expressions to match the process name.' % command)
        self.ctl.output('m%s a* - %ss all processes begining with "a".' % (command, command))

    def do_mstop(self, arg):
        self._expand_wildcards(arg, command='stop')
    def do_mstart(self, arg):
        self._expand_wildcards(arg, command='start')
    def do_mrestart(self, arg):
        self._expand_wildcards(arg, command='restart')
    def do_mstatus(self, arg):
        self._expand_wildcards(arg, command='status')
    def help_mstop(self):
        return self._wrap_help('stop')
    def help_mstart(self):
        return self._wrap_help('start')
    def help_mrestart(self):
        return self._wrap_help('restart')
    def help_mstatus(self):
        return self._wrap_help('status')

    # Graceful update of configuration
    def do_mightyreload(self, arg):
        def log(name, message):
            self.ctl.output("%s: %s" % (name, message))

        supervisor = self.ctl.get_supervisor()

        try:
            result = supervisor.reloadConfig()
        except xmlrpclib.Fault as e:
            self.ctl.exitstatus = LSBInitExitStatuses.GENERIC
            if e.faultCode == xmlrpc.Faults.SHUTDOWN_STATE:
                self.ctl.output('ERROR: already shutting down')
                return
            else:
                raise

        added, changed, removed = result[0]
        valid_gnames = set(arg.split())

        if "all" not in valid_gnames:
            valid_gnames = set()

        if valid_gnames:
            groups = set()
            for info in supervisor.getAllProcessInfo():
                groups.add(info['group'])
            groups.update(added)

        for gname in removed:
            if valid_gnames and gname not in valid_gnames:
                continue
            results = supervisor.stopProcessGroup(gname)
            log(gname, "stopped")

            fails = [res for res in results
                     if res['status'] == xmlrpc.Faults.FAILED]
            if fails:
                log(gname, "has problems; not removing")
                continue
            supervisor.removeProcessGroup(gname)
            log(gname, "removed process group")

        for gname in changed:
            if valid_gnames and gname not in valid_gnames:
                continue

            try:
                result = self.mightyreload.UpdateNumprocs(gname)
                result = json.loads(result)
                if result['type'] == 'reduce':
                    for process in result['processes_name']:
                        self.supervisor.stopProcess(process)
                        self.ctl.output(process + ' stoped')
                    for process in result['processes_name']:
                        process_name = process.split(':')[1]
                        self.mightyreload.removeProcessFromGroup(gname, process_name)
                        self.ctl.output(process + ' removed')
                elif result['type'] == 'add':
                    for process_name in result['processes_name']:
                        self.ctl.output(process_name + ' added')
                elif result['type'] == 'error':
                    self.ctl.output(result['msg'])
                    raise ValueError(result['msg'])
            except ValueError:
                results = supervisor.stopProcessGroup(gname)
                log(gname, "stopped")

                supervisor.removeProcessGroup(gname)
                supervisor.addProcessGroup(gname)
                log(gname, "updated process group")

        for gname in added:
            if valid_gnames and gname not in valid_gnames:
                continue
            supervisor.addProcessGroup(gname)
            log(gname, "added process group")

    def help_mightyreload(self):
        self.ctl.output("mightyreload\t\tGracefull update of supervisord configuration")


def make_mightyreload_controllerplugin(controller, **config):
    return MightyReloadControllerPlugin(controller, **config)

