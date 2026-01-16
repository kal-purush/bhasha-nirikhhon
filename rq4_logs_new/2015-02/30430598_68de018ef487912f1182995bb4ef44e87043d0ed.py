import numpy as np
import json
from subprocess import Popen, PIPE

def test_dspsr(**kwargs):
    """
    Run dspsr with a variety of inputs for benchmarking.

    example: t, ut, out, err = test_run(dm=5)

    kwargs and their defaults:
    (dspsr arguments)
      F - number of output channels [1024]
      T - length in seconds of pretend output file [10]
      cuda - comma-separated list of CUDA devices (as a string) ["0"]
      minram - minimum RAM usage in MB [1]
      D - dispersion measure [use DM from pulsar given in kwargs, otherwise this overrides it]
      minX - set desired FFT length as multiple of minimum length [unset, automatically chooses]
      t - number of CPU processor threads [unset, so 1]
    (header entries)
      source - pulsar to fold [1937+21]
      freq - centre frequency in MHz [600.0]
      nchan - number of input frequency channels [512]
      bw - observing bandwidth in MHz [400.0]
    (other options)
      print_cmd - print the dspsr call before running it [False]

    Returns: 
        - a dictionary of times (in s) that various parts of the process took
        - the total time (in s) as calculated by the unix "time" command
          (in principle, this is about the same as the values of the times above summed)
        - the stdout (from which the previous two items are parsed), split into lines
        - the stderr
    """
    if "F" not in kwargs: kwargs["F"] = 1024
    if "T" not in kwargs: kwargs["T"] = 10
    if "cuda" not in kwargs: kwargs["cuda"] = "0"
    if "minram" not in kwargs: kwargs["minram"] = 1

    if "source" not in kwargs: kwargs["source"] = "1937+21"
    if "freq" not in kwargs: kwargs["freq"] = 600.
    if "nchan" not in kwargs: kwargs["nchan"] = 512
    if "bw" not in kwargs: kwargs["bw"] = 400.
    
    arg_dict = {}
    arg_dict["-F"] = "%d:D" % kwargs["F"]
    arg_dict["-r"] = None
    arg_dict["-cuda"] = str(kwargs["cuda"])
    arg_dict["-T"] = str(kwargs["T"])
    arg_dict["-minram"] = str(kwargs["minram"])
    if "D" in kwargs:
        arg_dict["-D"] = str(kwargs["D"])
    if "minX" in kwargs:
        arg_dict["-x"] = "minX%d" % kwargs["minX"]
    if "t" in kwargs:
        arg_dict["-t"] = str(kwargs["t"])

    cmd = ["time", "-f", "\"TOTAL WALL TIME ELAPSED: %e seconds\"", "dspsr"]
    for k in arg_dict.keys():
        cmd.append(k)
        if arg_dict[k] is not None:
            cmd.append(arg_dict[k])

    cmd.append("-header")
    cmd.append("DUMMY")
    cmd.append("HDR_VERSION=1.0")
    cmd.append("INSTRUMENT=Dummy")
    cmd.append("TELESCOPE=1")
    cmd.append("SOURCE=%s" % kwargs["source"])
    cmd.append("MODE=PSR")
    cmd.append("FREQ=%s" % str(kwargs["freq"]))
    cmd.append("NCHAN=%d" % kwargs["nchan"])
    cmd.append("NPOL=2")
    cmd.append("NBIT=8")
    cmd.append("NDIM=2")
    cmd.append("TSAMP=%f" % (kwargs["nchan"] / float(kwargs["bw"])))
    cmd.append("BW=%s" % str(kwargs["bw"]))
    cmd.append("UTC_START=2014-06-04-12:00:00")
    cmd.append("OFFSET=0")
    cmd.append("MAX_DATA_MB=4096")

    dspsr_call = ""
    # skip the 'time' parts
    for item in cmd[cmd.index('dspsr'):]:
        dspsr_call += item + " "

    if 'print_cmd' in kwargs:
        if kwargs['print_cmd']:
            print dspsr_call

    ex = Popen(cmd, stdout=PIPE, stderr=PIPE)
    ex_err, ex_out = ex.communicate()

    # get rid of the progress text
    ex_out = ex_out.split('\n')
    for line in ex_out:
        if "Finished 0 s" in line:
            ex_out.remove(line)

    timed_parts = ["Dummy", "DummyUnpacker", "CUDA::Transfer", "Filterbank", "Detection", "Fold"]
    times = {}
    unix_time = -1.

    # put the times from the various parts of the code into a dictionary
    for line in ex_out:
        split_line = line.split()
        if len(split_line):
            if split_line[0] in timed_parts:
                times[split_line[0]] = float(split_line[1])
            # make sure to get the preparation time and unloading time as well
            elif "dspsr: prepared in" in line:
                times["Preparation"] = float(split_line[3])
            elif "dsp::Archiver::unload in" in line:
                times["Unloading"] = float(split_line[2])
            elif "TOTAL WALL TIME ELAPSED" in line:
                unix_time = float(split_line[4])

    return times, unix_time, ex_out, ex_err, dspsr_call

class Trials:

    timed_parts = ["Dummy", "DummyUnpacker", "CUDA::Transfer", "Filterbank",\
        "Detection", "Fold", "Preparation", "Unloading"]

    def __init__(self, varied_arg, varied_arg_values, fixed_args={}):
        """
        varied_arg: string identifier for argument being varied
        varied_arg_values: an iterable of values to walk through
        fixed_args: a dictionary whose keys are string identifiers for
          arguments being held fixed and whose values are the fixed value--
          any arguments left out are fixed at their default values
        """
        self.varied_arg = varied_arg
        self.varied_arg_values = varied_arg_values
        self.fixed_args = fixed_args
        self.executed = False
        self.all_times = {}
        for part in self.timed_parts:
            self.all_times[part] = []
        self.all_utime = []
        self.all_stdout = []
        self.all_stderr = []
        self.all_dspsr_calls = []
        self.comment = ""

    def execute(self):
        if self.executed:
            print "Already executed."
        else:
            for ii, arg_val in enumerate(self.varied_arg_values):
                print "Run %d of %d: %s = %d" % (ii+1, len(self.varied_arg_values), self.varied_arg, arg_val)
                test_dspsr_args = dict(self.fixed_args)
                test_dspsr_args[self.varied_arg] = arg_val
                test_dspsr_args["print_cmd"] = True
                t, ut, out, err, dspsr_call = test_dspsr(**test_dspsr_args)
                if len(t) == len(self.timed_parts):
                    for part in self.timed_parts:
                        self.all_times[part].append(t[part])
                    self.all_utime.append(ut)
                else:
                    for part in self.timed_parts:
                        self.all_times[part].append(-1.)
                    self.all_utime.append(-1.)
                self.all_stdout.append(out)
                self.all_stderr.append(err)
                self.all_dspsr_calls.append(dspsr_call)
                print "That run took %.2f seconds." % ut
            print "Done."
            self.executed = True

    def add_comment(self, comment):
        self.comment = comment

    def save_results(self, filename):
        if not self.executed:
            print "Run execute() method first to get results."
        else:
            with open(filename, 'w') as f:
                json.dump({"times":self.all_times,\
                    "utime":self.all_utime, "stdout":self.all_stdout,\
                    "stderr":self.all_stderr, "comment":self.comment,\
                    "varied_arg":self.varied_arg,\
                    "varied_arg_values":list(self.varied_arg_values),\
                    "fixed_args":self.fixed_args,\
                    "dspsr_calls":self.all_dspsr_calls}, f)

#    def plot_results(self):
#        pass