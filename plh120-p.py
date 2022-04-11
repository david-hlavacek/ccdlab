#!/usr/bin/env python

import datetime
import numpy as np
import re

from daemon import SimpleFactory, SimpleProtocol, catch


class DaemonProtocol(SimpleProtocol):
    _debug = False  # Display all traffic for debug purposes

    @catch
    def processMessage(self, string):
        cmd = SimpleProtocol.processMessage(self, string)
        if cmd is None:
            return
        obj = self.object
        hw = obj['hw']
        string = string.strip()
        STRING = string.upper()
        while True:
            if string == 'get_status':
                self.message('status hw_connected=%s Voltage=%g VoltageActual=%g Current_Limit=%g CurrentActual=%g Vstatus=%i OVP1=%s OCP1=%s' %
                             (self.object['hw_connected'],
                              self.object['V1'],
                              self.object['V1O'],
                              self.object['I1'],
                              self.object['I1O'],
                              self.object['VOut1'],
                              self.object['OVP1'],
                              self.object['OCP1'],))
                break

            regex0 = re.compile(r'\:?ENGAGE')
            regex1 = re.compile(r'\:?OP1.1')
            match = re.match(regex0, STRING)
            if not match:
                match = re.match(regex1, STRING)
            if match:
                hw.messageAll('OP1 1\n', type='hw', keep=False, source=self.name)
                obj['Vstatus'] = 1
                break

            regex0 = re.compile(r'\:?DISENGAGE')
            regex1 = re.compile(r'\:?OP1.0')
            match = re.match(regex0, STRING)
            if not match:
                match = re.match(regex1, STRING)
            if match:
                hw.messageAll('OP1 0\n', type='hw', keep=False, source=self.name)
                obj['Vstatus'] = 0
                break

            if STRING[-1] == '?':
                hw.messageAll(string, type='hw', keep=True, source=self.name)
            else:
                hw.messageAll(string, type='hw', keep=False, source=self.name)
            break


class plh120_Protocol(SimpleProtocol):
    _debug = False  # Display all traffic for debug purposes
    _refresh = 0.01

    def __init__(self):
        SimpleProtocol.__init__(self)
        self.commands = []  # Queue of command sent to the device which will provide replies, each entry is a dict with keys "cmd","source","timeStamp"
        self.status_commands = ['I1?', 'V1?',
                                'I1O?', 'V1O?',
                                'OP1?', 'OVP1?', 'OCP1?',
                                'CONFIG?', ]
        #self.status_commands = []
        self.name = 'hw'
        self.type = 'hw'
        self.lastAutoRead = datetime.datetime.utcnow()

    @catch
    def connectionMade(self):
        SimpleProtocol.connectionMade(self)
        self.commands = []
        # We will set this flag when we receive any reply from the device
        self.object['hw_connected'] = 1
        SimpleProtocol.message(self, '*RST')

    @catch
    def connectionLost(self, reason):
        self.commands = []
        SimpleProtocol.connectionLost(self, reason)
        resetObjStatus(self.object)

    @catch
    def processMessage(self, string):
        obj = self.object  # Object holding the state
        if self._debug:
            print('PLH120-P >> %s' % string)
        # Update the last reply timestamp
        obj['hw_last_reply_time'] = datetime.datetime.utcnow()
        obj['hw_connected'] = 1
        # Process the device reply
        while len(self.commands):
            ccmd = self.commands[0]['cmd'].decode()
            # We have some sent commands in the queue - let's check what was the oldest one
            br = False
            if ccmd == 'I1?':
                obj['Current_Limit'] = float(string[3:-1])
                br = True
                break
            if ccmd == 'V1?':
                obj['Voltage'] = float(string[3:-1])
                br = True
                break
            if ccmd == 'V1O?':
                obj['VoltageActual'] = float(string[0:-2])
                br = True
                break
            if ccmd == 'I1O?':
                obj['CurrentActual'] = float(string[0:-2])
                br = True
                break
            if ccmd == 'OP1?':
                obj['VOut1'] = int(string[0])
                br = True
                break
            if ccmd == 'OVP1?':
                obj['OVP1'] = float(string[:-1].split()[0])
                br = True
                break
            if ccmd == 'OCP1?':
                obj['OCP1'] = float(string[:-1].split()[0])
                br = True
                break                
            if br:
                break

            # some more commands
            break
        else:
            return
        if not self.commands[0]['source'] == 'itself':
            # in case the origin of the query was not itself, forward the answer to the origin
            obj['daemon'].messageAll(string, self.commands[0]['source'])
        self.commands.pop(0)

    @catch
    def update(self):
        if self._debug:
            print('--------self.commands--------------')
            for cc in self.commands:
                print(cc)
            print('----------------------')
        # first check if device is hw_connected
        if self.object['hw_connected'] == 0:
            # if not connected do not send any commands
            return

        if len(self.commands) and not self.commands[0]['sent']:
            SimpleProtocol.message(self, self.commands[0]['cmd'])
            if not self.commands[0]['keep']:
                self.commands.pop(0)
            else:
                self.commands[0]['sent'] = True
        elif not len(self.commands):
            for k in self.status_commands:
                self.commands.append({'cmd': k.encode('ascii'), 'source': 'itself', 'keep': True, 'sent': False})

    @catch
    def message(self, string, keep=False, source='itself'):
        """
        Send the message to the controller. If keep=True, expect reply
        """
        n = 0
        for cc in self.commands:
            if not cc['sent']:
                break
            n += 1
        if self._debug:
            print('cmd', string, 'from', source, 'will be inserted at', n)
        self.commands.insert(n, {'cmd': string, 'source': source, 'keep': keep, 'sent': False})


def resetObjStatus(obj):
    obj['hw_connected'] = 0
    obj['I1'] = np.nan
    obj['V1'] = np.nan
    obj['I1O'] = np.nan
    obj['V1O'] = np.nan
    obj['VOut1'] = 0
    obj['OVP1'] = np.nan
    obj['OCP1'] = np.nan    


if __name__ == '__main__':
    from optparse import OptionParser
    parser = OptionParser(usage="usage: %prog [options] arg")
    parser.add_option('-H', '--hw-host', help='Hardware host to connect', action='store', dest='hw_host', default='192.168.1.6')
    parser.add_option('-P', '--hw-port', help='Hardware port to connect', action='store', dest='hw_port', type='int', default=9221)
    parser.add_option('-p', '--port', help='Daemon port', action='store', dest='port', type='int', default=7026)
    parser.add_option('-n', '--name', help='Daemon name', action='store', dest='name', default='plh120-p')
    parser.add_option("-D", '--debug', help='Debug mode', action="store_true", dest="debug")
    (options, args) = parser.parse_args()
    # Object holding actual state and work logic.
    # May be anything that will be passed by reference - list, dict, object etc
    obj = {}
    resetObjStatus(obj)

    # Factories for daemon and hardware connections
    # We need two different factories as the protocols are different
    daemon = SimpleFactory(DaemonProtocol, obj)
    hw = SimpleFactory(plh120_Protocol, obj)
    if options.debug:
        daemon._protocol._debug = True
        hw._protocol._debug = True
    daemon.name = options.name
    obj['daemon'] = daemon
    obj['hw'] = hw
    obj['hw_last_reply_time'] = datetime.datetime(1970, 1, 1)  # Arbitrarily old time moment
    # Incoming connections
    daemon.listen(options.port)
    # Outgoing connection
    hw.connect(options.hw_host, options.hw_port)
    daemon._reactor.run()
