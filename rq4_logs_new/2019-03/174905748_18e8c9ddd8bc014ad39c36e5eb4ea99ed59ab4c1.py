from libnmap.parser import NmapParser, NmapParserException
from libnmap.process import NmapProcess
from prompt_toolkit import prompt, print_formatted_text
from prompt_toolkit.history import FileHistory
from terminaltables import AsciiTable

from troy_platform.common.cli.Style import style
from troy_platform.common.cli.completer import NestedCompleter

history = FileHistory('prompt_history/info_gathering/NMap')


class NMap:
    """
    NMap scanner.
    Dependencies:Nmap("Network Mapper") must be install in your OS
    and add this command to the path path.
    """

    def __init__(self):
        self.rhosts = '127.0.0.1'
        self.options = '-sT'
        self.words_dic = {
            'show': ['options'],
            'set': {'rhosts': None,
                    'options': ['-sS', '-sT', '-sU', '-sY', '-sN', '-sF', '-sX', '-sA', '-sW', '-sM', '-sZ', '-sO',
                                '-sP', '-sI',
                                '-sR', '-sL', '-p 80']},
            'run': None,
        }
        self.meta_dict = {
            'show': 'show info',
            'set': 'set nmap options',
            'run': 'run',
            '-sS': 'TCP SYN Scan',
            '-sT': 'TCP Connect Scan',
            '-sU': 'UDP Scan',
            '-sY': 'SCTP INIT Scan',
            '-sN': 'TCP Null Scan',
            '-sF': 'TCP FIN Scan',
            '-sX': 'TCP Xmas Scan',
            '-sA': 'TCP ACK Scan',
            '-sW': 'TCP Window Scan',
            '-sM': 'TCP Maimon Scan',
            '-sZ': 'SCTP Cookie Echo Scan',
            '-sO': 'IP Protocol Scan',
            '-sP': 'Ping Scan',
            '-sI': 'Idle Scan',
            '-sR': 'RPC Scan',
            '-sL': 'List Scan',
            '-p': 'Scan port',
        }

    def start(self):
        """
        """
        while True:
            try:
                cmd = prompt('(nmap)>>>', history=history,
                             completer=NestedCompleter(words_dic=self.words_dic,
                                                       meta_dict=self.meta_dict), style=style)
            except KeyboardInterrupt:
                print_formatted_text("ControlC!")
                break
            except EOFError:
                print_formatted_text("ControlD!")
                break
            cmd = str(cmd).strip()
            if len(cmd) == 0:
                continue
            if ' ' in cmd:
                cmd_arr = cmd.split(' ', maxsplit=1)
                cmd = cmd_arr[0]
                arg = cmd_arr[1]
            else:
                arg = None
            cmd = cmd.lower()
            if cmd == 'set':
                self.__set_parameter(arg)
            elif cmd == 'show':
                self.__show(arg)
            elif cmd == 'run':
                self.__run()
            else:
                print_formatted_text('Unknown Command!!')

    def __show(self, flag):
        if str(flag).strip().lower() == 'options':
            info_table = [
                ['\nname', '\nCurrent Setting', '\nRequired', '\nDescription'],
                ['RHOSTS', self.rhosts, 'no', 'Default is 127.0.0.1'],
                ['options', self.options, 'no', 'Default is -sT'],
            ]
            table = AsciiTable(info_table, "Module Options")
            print_formatted_text("")
            print_formatted_text(table.table)
            print_formatted_text("")
        else:
            print_formatted_text("Unknown Command " + flag)

    def __set_parameter(self, parameter):
        parameter_arr = str(parameter).strip().split(" ", maxsplit=1)
        if len(parameter_arr) == 2:
            try:
                if parameter_arr[0].lower() == 'rhosts':
                    self.rhosts = str(parameter_arr[1])
                elif parameter_arr[0].lower() == 'options':
                    self.options = str(parameter_arr[1])
            except Exception as e:
                print_formatted_text(e)
        else:
            print_formatted_text('set parameter error')

    def __run(self):
        try:
            nm = NmapProcess(self.rhosts, options=self.options, event_callback=self.__run_callback)
            rc = nm.run()
            if rc != 0:
                print_formatted_text("nmap scan failed: {0}".format(nm.stderr))
            else:
                try:
                    report = NmapParser.parse(nm.stdout)
                    NMap.__print_report(report)
                except NmapParserException as e:
                    print_formatted_text("Exception raised while parsing scan: {0}".format(e.msg))
        except Exception as e:
            print_formatted_text(e)

    @staticmethod
    def __print_report(nmap_report):
        print_formatted_text("Starting Nmap {0} ( http://nmap.org ) at {1}".format(
            nmap_report.version,
            nmap_report.started))

        for host in nmap_report.hosts:
            if len(host.hostnames):
                tmp_host = host.hostnames.pop()
            else:
                tmp_host = host.address

            print_formatted_text("")
            print_formatted_text("Nmap scan report for {0} ({1})".format(
                tmp_host,
                host.address))
            print_formatted_text("Host is {0}.".format(host.status))

            res_table = [
                ['\nPORT', '\nprotocol', '\nSTATE', '\nSERVICE'],
            ]
            for service in host.services:
                res_table.append([str(service.port), service.protocol, service.state, service.service])
            table = AsciiTable(res_table)
            print_formatted_text(table.table)
        print_formatted_text(nmap_report.summary)

    @staticmethod
    def __run_callback(a):
        task = a.current_task
        if task:
            print("Task {0} ({1}): ETC: {2} DONE: {3}%".format(task.name,
                                                               task.status,
                                                               task.etc,
                                                               task.progress))


if __name__ == '__main__':
    NMap().start()