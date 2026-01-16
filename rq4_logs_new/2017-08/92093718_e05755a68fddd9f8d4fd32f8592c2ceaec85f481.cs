using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CommandLine;
using CommandLine.Text;

namespace CRMLastLoginReport
{
    public class CommandLineArgs
    {
        [Option('c',"connection", Required = true, HelpText = "Connection string for the target CRM environment")]
        public string ConnectionString { get; set; }
        [Option("ignoredeactivated",DefaultValue = false , HelpText = "Whether or not we should ignore disabled users")]
        public bool IgnoreDeactivated { get; set; }

        [HelpOption]
        public string GetUsage()
        {
            return HelpText.AutoBuild(this, (HelpText current) => HelpText.DefaultParsingErrorsHandler(this, current));
        }
    }
}