# -*- coding: utf-8 -*-
from supervisor.supervisorctl import ControllerPluginBase
from supervisor import xmlrpc
from supervisor_mightyreload.contrib import LSBInitExitStatuses

import xmlrpclib
import json


class MightyReloadControllerPlugin(ControllerPluginBase):
    name = 'mightyreload'

    def __init__(self, controller, **config):
        self.ctl = controller
        self.supervisor = controller.get_server_proxy('supervisor')
        self.mightyreload = controller.get_server_proxy('mightyreload')

    # Graceful update of configuration
    def do_mreload(self, arg):
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

    def help_mreload(self):
        self.ctl.output("mreload\t\tGracefull update of supervisord configuration")


def make_mightyreload_controllerplugin(controller, **config):
    return MightyReloadControllerPlugin(controller, **config)

