import argparse 
import sys
import os
import json
import SimpleHTTPServer
import SocketServer
import threading
import readline
import sqlite3
from subprocess import call
import re
import requests
import pty

class ExitApplicationException(Exception):
  pass

class Settings:
  machine = ""
  network = ""
  pwd =  ""
  debug = False
  win_server_dir = None
  linux_server_dir = None

  def __init__(self, machine="", network="", pwd="", debug=False, win_server_dir=None, linux_server_dir=None):
    self.machine = machine
    self.network = network
    self.pwd = pwd
    self.debug = debug
    self.win_server_dir = win_server_dir
    self.linux_server_dir = win_server_dir

class Commands:
  settings = None
  env = None
  found = None

  win_server = None
  linux_server = None

  def __init__(self, settings, env):
    self.settings = settings
    self.env = env
    self.found = []

    if not settings.network:
      self.settings.network = raw_input("Enter Network> ")

    if not settings.machine:
      self.settings.machine = raw_input("Enter Machine> ")

    self.do_network(settings.network)
    self.do_machine(settings.machine)

  def do_help(self):
    print " - help"
    print " - machine <machine name>"
    print " - network <network name>"
    print " - tag <tag> <value>"
    print " - rtag <tag>"
    print " - find <tag>[=<value>]"
    print " - go <found id|network/machine|machine>"
    print " - show <loot|settings|files|tags|machines|networks|scan|notes>"
    print " - scan <command>"
    print " - notes [<filename>]"
    print " - exploit <http:|ftp:|filename>"
    print " - ed <env/file>"
    print " - winsvr <host> <port>"
    print " - linuxsvr <host> <port>"
    print " - exit|quit"
  
  def do_exit(self, args):

    if len(args) == 0:
      raise ExitApplicationException()
    if args[0] == "winsvr":
      self.win_server.shutdown()
    elif args[0] == "linuxsvr":
      self.linux_server.shutdown()

  def do_machine(self, machine):
    self.settings.machine = machine

    self.do_machine_init()

  def do_network(self, network):
    self.settings.network = network

    path = "{}/{}".format(self.settings.pwd, network)

    if not os.path.isdir(path):
      os.makedirs(path)

  def do_win_server(self, ipaddr, port):
    if not self.settings.linux_server_dir:
      entered_win_server_dir = raw_input("Windows web server dir: ")
      self.settings.win_server_dir = entered_win_server_dir

    os.chdir(self.settings.win_server_dir)
    server = SimpleHTTPServer.SimpleHTTPRequestHandler
    httpd = SocketServer.TCPServer((ipaddr, port), server)

    thread = threading.Thread(target = httpd.serve_forever)
    thread.daemon = True
    
    thread.start()
    self.win_server = httpd

    print "Server now running on http://{}:{}".format(ipaddr, port)

  def do_linux_server(self, ipaddr, port):
    if not self.settings.linux_server_dir:
      entered_linux_server_dir = raw_input("Linux web server dir: ")
      self.settings.linux_server_dir = entered_linux_server_dir

    os.chdir(self.settings.linux_server_dir)
    server = SimpleHTTPServer.SimpleHTTPRequestHandler
    httpd = SocketServer.TCPServer((ipaddr, port), server)

    thread = threading.Thread(target = httpd.serve_forever)
    thread.daemon = True
    
    thread.start()
    self.linux_server = httpd

    print "Server now running on http://{}:{}".format(ipaddr, port)

  def do_show(self, args):
    for arg in args:
      if arg == "loot":
        for f in os.listdir(self.path_gen("loot")):
          print f
      elif arg == "scan":
        for f in os.listdir(self.path_gen("scan")):
          with open(self.path_gen("scan/" + f)) as r:
            print r.read()
      elif arg == "notes":
        for f in os.listdir(self.path_gen("notes")):
          with open(self.path_gen("notes/" + f)) as r:
            print r.read()
      elif arg == "exploit":
        for f in os.listdir(self.path_gen("exploit")):
          print f
      elif arg == "settings":
        print json.dumps(vars(self.settings), indent=3)
      elif arg == "networks":
        for f in os.listdir(self.path_gen("../../")):
          print f
      elif arg == "machines":
        for f in os.listdir(self.path_gen("../")):
          if os.path.isdir(f):
            print f
      elif arg == "files":
        for root, dirs, files in os.walk(self.path_gen(".")):
          path = root.split(os.sep)
          basename = os.path.basename(root)
        
          if basename == ".": continue

          print basename
          for file in files:
            print " " * 2 + file
      elif arg == "tags":
        r = self.env.execute("SELECT tag, value FROM tag_map m LEFT JOIN tags t ON m.tag_id = t.id WHERE network = ? AND machine = ?",
          [self.settings.network, self.settings.machine]
        )

        for row in r:
          print "{} => {}".format(row[0], row[1])

  def do_machine_init(self):
    dirs = ["loot", "scan", "notes", "exploit"]

    # Create directory structure
    for d in dirs:
      path = self.path_gen(d)

      if os.path.exists(path):
        continue

      self.debug("Creating {}".format(str(path)))
      os.makedirs(path)

  def do_tag_add(self, name, value):
    c = self.env.cursor()

    tag_exists = c.execute("SELECT id FROM tags WHERE tag = ?", (name,)).fetchone()

    if not tag_exists:
      tag_id = c.execute("INSERT INTO tags (tag) VALUES (?)", (name,)).lastrowid
    else:
      tag_id = tag_exists[0]

    c.execute("INSERT INTO tag_map (network, machine, tag_id, value) VALUES (?, ?, ?, ?)", 
      (self.settings.network, self.settings.machine, tag_id, value)
    )

    self.env.commit()

  def do_tag_remove(self, name):
    c = self.env.cursor()

    r = c.execute("SELECT id FROM tags WHERE tag = ?", [name,]).fetchone()
    c.execute("DELETE FROM tag_map WHERE tag_id = ? AND network = ? AND machine = ?",
      [r[0], self.settings.network, self.settings.machine]
    )

    self.env.commit()

  def do_tag_find(self, tag):
    if "=" in tag:
      (tag, value) = tag.split("=")
      r = self.env.execute("SELECT network, machine FROM tag_map m LEFT JOIN tags t ON m.tag_id = t.id WHERE t.tag = ? AND value = ?", (tag, value))
    else:
      r = self.env.execute("SELECT network, machine FROM tag_map m LEFT JOIN tags t ON m.tag_id = t.id WHERE t.tag = ?", (tag,))


    idx = 0

    for row in r:
      self.found.append([row[0], row[1]])

      print "[{}] {}".format(idx, "{}/{}".format(row[0], row[1]))
      idx += 1

  def do_go(self, search):
    if(search.isdigit()):
      idx = int(search)
      if idx >= 0 and idx <= len(self.found)-1:
        self.do_network(self.found[idx][0])
        self.do_machine(self.found[idx][1])
    elif "/" in search:
      (network, machine) = search.split("/")

      self.do_network(network)
      self.do_machine(machine)
    else:
      self.do_machine(search)

  def do_exploit(self, exploit):
    if exploit.startswith("http://") or exploit.startswith("https://"):
      src = requests.get(exploit).text
      with open(self.path_gen("exploit/" + os.path.basename(exploit)), "w") as f:
        f.write(src)
        
    elif exploit.startswith("ftp://"):
      pass

  def do_edit(self, selected):
    path = self.path_gen(selected)

    call(["/usr/bin/vim", path])
    
  def do_sh_scan(self, command):
    scan_name = self.cmd_to_filename(command)

    if "{}" not in command:
      command += " " + self.settings.machine
    else:
      command = command.replace("{}", self.settings.machine)

    path = self.path_gen("scan/" + scan_name) + ".txt"

    call(command + " > {} && vim {}".format(path, path), shell=True)

  def do_notes(self, args):
    if args:
      filename = "notes/" + args[0]
    else:
      filename = "notes/general.md"

    self.do_edit(filename)

  def debug(self, msg):
    if self.settings.debug:
      print msg

  def get_settings(self):
    return self.settings

  def path_gen(self, selected):
     return "{}/{}/{}/{}".format(
      self.settings.pwd, 
      self.settings.network,
      self.settings.machine,
      selected)

  def cmd_to_filename(self, command):
    return re.sub(r'[^a-zA-Z0-9]+', "-", command)

  def process_command(self, cmd):
    if not cmd:
      return

    cmds = {
      "help": lambda cmd, args : self.do_help(),
      "machine": lambda cmd, args : self.do_machine(args[0]),
      "network": lambda cmd, args : self.do_network(args[0]),
      "show": lambda cmd, args : self.do_show(args),
      "notes": lambda cmd, args : self.do_notes(args),
      "exploit": lambda cmd, args : self.do_exploit(args[0]),
      "winsvr": lambda cmd, args : self.do_win_server(args[0], int(args[1])),
      "linuxsvr": lambda cmd, args : self.do_linux_server(args[0], int(args[1])),
      "tag": lambda cmd, args : self.do_tag_add(args[0], args[1]),
      "rtag": lambda cmd, args : self.do_tag_remove(args[0]),
      "find": lambda cmd, args : self.do_tag_find(args[0]),
      "ed": lambda cmd, args : self.do_edit(args[0]),
      "go": lambda cmd, args : self.do_go(args[0]),
      "scan": lambda cmd, args : self.do_sh_scan(" ".join(args)),
      "exit": lambda cmd, args : self.do_exit(args),
      "quit": lambda cmd, args : self.do_exit(args)
    }

    cmd_split = cmd.split()
    try:
      execute = cmds.get(cmd_split[0])
      execute(cmd_split[0], cmd_split[1:])
    except ExitApplicationException:
      raise
    except Exception as e:
      self.debug(str(e))
      self.do_help()

def header():
    print """
,---.          --.--          |        --.--          |
|---',---.,---.  |  ,---.,---.|---       |  ,---.,---.|
|    |---'|   |  |  |---'`---.|          |  |   ||   ||    
`    `---'`   '  `  `---'`---'`---'      `  `---'`---'`---'

Author: Ed Cradock
Version: 0.1
"""

def setup_env(settings):
  conn = sqlite3.connect("{}/.env.db".format(settings.pwd))
  c = conn.cursor()

  c.execute("CREATE TABLE IF NOT EXISTS tags (id INTEGER PRIMARY KEY , tag TEXT)")
  c.execute("CREATE TABLE IF NOT EXISTS tag_map (network TEXT, machine TEXT, tag_id INTEGER, value TEXT)")

  conn.commit()

  return conn

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("-w", "--working-dir", help="Current working directory")
  parser.add_argument("-n", "--network", help="Network name")
  parser.add_argument("-m", "--machine", help="Machine name")
  parser.add_argument("-d", "--debug", help="Enable debug messages", const=True, default=False, nargs="?")

  args = parser.parse_args()
  settings = Settings()

  if args.network:
    settings.network = args.network

  if args.machine:
    settings.machine = args.machine

  if args.debug:
    settings.debug = args.debug

  if args.working_dir:
    settings.pwd = args.working_dir
  else:
    settings.pwd = os.getcwd()

  header()
  env = setup_env(settings)
  commands = Commands(settings, env)
  
  while 1:
    s = raw_input("{}/{} > ".format(settings.network, settings.machine))

    try:
      commands.process_command(s)

      settings = commands.get_settings()
    except ExitApplicationException as e:
      return 

if __name__ == "__main__":
  main()