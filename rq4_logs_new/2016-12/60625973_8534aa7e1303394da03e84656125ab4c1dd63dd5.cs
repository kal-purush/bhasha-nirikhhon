





















using System.Collections;
using System.Collections.Generic;
using DocoptNet;

namespace EtlPackage
{

    // Generated class for Main.usage.txt
    public class MainArgs
    {
        public const string USAGE = @"SqlServerUtilities.

	Usage:
	  SqlServerUtilities.exe [--map|--markdown=<markdown_filename>] [--packagepath=<packagepath>] [--server=<server_name>] (--msdb|--local)

	Options:
	  -h --help                        Show this screen.
	  --markdown=<markdown_filename>   readme markdown full path filename [default: readme]
	  --map                            indicates Map.PackageTable table will be populated from package dataflow destination tables.
	  --msdb                           indicates package is deployed to SSIS MSDB folder.  (packagepath specifies MSDB path)
	  --local                          indicates package exists on file system.  (packagepath specifies file system path)
	  --packagepath=<packagepath>      path of package or folder containing package(s).  can be local file system path or SSIS MSDB path.
	  -server=<server_name>            this is the server.
";
        private readonly IDictionary<string, ValueObject> _args;
        public MainArgs(ICollection<string> argv, bool help = true,
                                                      object version = null, bool optionsFirst = false, bool exit = false)
        {
            _args = new Docopt().Apply(USAGE, argv, help, version, optionsFirst, exit);
        }

        public IDictionary<string, ValueObject> Args
        {
            get { return _args; }
        }

        public bool OptMap { get { return _args["--map"].IsTrue; } }
        public string OptMarkdown { get { return null == _args["--markdown"] ? null : _args["--markdown"].ToString(); } }
        public string OptPackagepath { get { return null == _args["--packagepath"] ? null : _args["--packagepath"].ToString(); } }
        public string OptServer { get { return null == _args["--server"] ? null : _args["--server"].ToString(); } }
        public bool OptMsdb { get { return _args["--msdb"].IsTrue; } }
        public bool OptLocal { get { return _args["--local"].IsTrue; } }

    }


}
