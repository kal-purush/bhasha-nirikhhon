# create_internal_jars_pom.py
#
# Usage:
#   python3 tools/create_internal_jars_pom.py <matlab_release>
#
# This is a tool to create Maven dependency definitions for all the internal
# JARs in a Matlab distribution. (The "internal" JARs are those written by 
# MathWorks as part of Matlab and found in java/jar, as opposed to the third-
# party JARs found in java/jarext.)
#
# This must be run from the root of the repo.

# TODO: Figure out how to handle versions. Just require user input, since
# the version increment is a manual decision?

import os
import platform
import subprocess
import sys

matlab_release = sys.argv[1]

pom_version = '0.1.0-SNAPSHOT'

def maybe_makedirs(dir):
  if not os.path.exists(dir):
    os.makedirs(dir)

def detect_matlab_version(matlab_root, os_tag):
  matlab_exe = matlab_root + '/bin/matlab'
  proc = subprocess.run([matlab_exe, '-nodesktop', '-nodisplay', '-r', 
    "fprintf('%s', version); exit"], capture_output = True, text = True)
  out = proc.stdout
  lines = out.splitlines()
  last_line = lines[-1]
  ix_space = last_line.find(' ')
  ver_str = last_line[:ix_space]
  # Strip the build portion of the version; we're not building artifacts that specific
  ix_last_dot = ver_str.rfind('.')
  ver_str2 = ver_str[:ix_last_dot]
  return ver_str2

if platform.system() == 'Linux':
  os_tag = 'linux'
  os_name = 'Linux'
  raise Exception("Linux is not supported yet.")
elif platform.system() == 'Darwin':
  os_tag = 'mac'
  os_name = 'macOS'
  matlab_root = "/Applications/MATLAB_"+matlab_release+".app"
elif platform.system() == 'Windows':
  os_tag = 'win'
  os_name = 'Windows'
else:
  raise Exception('Unrecognized platform value: ' + platform.system())

matlab_version = detect_matlab_version(matlab_root, os_tag)
print(f'Detected Matlab version: {matlab_version}')

tag = matlab_release + '-' + os_tag
out_dir = f'internal-jars/{tag}'
maybe_makedirs(out_dir)
out_file = f'{out_dir}/pom.xml'
print(f'Creating {out_file}')
f = open(out_file, 'w')

matlab_jar_dir = matlab_root + "/java/jar"
skips = ["zh_CN", "ja_JP", "ko_KR"]

f.write(f"""<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" 
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>

  <groupId>net.janklab.jump</groupId>
  <artifactId>matlab-internal-jars-{tag}</artifactId>
  <version>{pom_version}</version>
  <packaging>pom</packaging>

  <!--
    This POM contains references to Matlab's internal Java libraries.
  	DANGER! These are undocumented internal features! Using this stuff may
  	well cause your code to break at any time!
  -->

  <name>Matlab-JUMP matlab-internal-jars: {matlab_release}-{os_tag}</name>
  <description>Matlab internal JAR refs for Matlab {matlab_release} on {os_name}</description>
  <url>https://github.com/apjanke/matlab-jump</url>
  <issueManagement>
    <system>GitHub</system>
    <url>https://github.com/apjanke/matlab-jump/issues</url>
  </issueManagement>
  <scm>
    <url>https://github.com/apjanke/matlab-jump</url>
  </scm>

""")

f.write('  <dependencies>\n')
for root, subdirs, files in os.walk(matlab_jar_dir):
  rel_dir = root[len(matlab_jar_dir)+1:]

  if len(rel_dir) >= 5:
    first_five = rel_dir[:5]
    if first_five in skips:
      #print('-> Skipping locale dir');
      continue

  for filename in files:
    if not filename.endswith(".jar"):
      continue
    file_path = os.path.join(root, filename)
    #print('\t- file %s (full path: %s)' % (filename, file_path))
    qual = rel_dir.replace("/", ".")
    jar_base = filename[:-4]
    artifact = qual + "." + jar_base if qual else jar_base
    f.write('    <dependency>\n')
    f.write('      <groupId>net.janklab.jump.matlab-internal-jars</groupId>\n')
    f.write('      <artifactId>%s-%s</artifactId>\n' % (artifact, matlab_release))
    f.write('      <version>%s</version>\n' % (matlab_version))
    f.write('      <scope>system</scope>\n')
    f.write('      <systemPath>%s</systemPath>\n' % (file_path))
    f.write('    </dependency>\n')

f.write('  </dependencies>\n')
f.write('</project>\n')

f.close()