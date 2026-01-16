using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CommandLine;

namespace SqlDataScripter
{
    class Options
    {
        [Option('s', "server", HelpText = "SQL Server instance.", Default = ".")]
        public string Server { get; set; }

        [Option('u', "user", Required = false, HelpText = "SQL user name.")]
        public string UserName { get; set; }

        [Option('p', "password", Required = false, HelpText = "SQL user password.")]
        public string Password { get; set; }

        [Option('d', "database", Required = true, HelpText = "Script database name.")]
        public string Database { get; set; }

        [Option('o', "output", Required = false, HelpText = "Output file name.")]
        public string OutputFileName { get; set; }

        [Option('t', "table", Required = false)]
        public IEnumerable<string> Tables { get; set; }

        [Option("schema", HelpText = "Script table schema.")]
        public bool ScriptSchema { get; set; }

        [Option("data", HelpText = "Script table data.")]
        public bool ScriptData { get; set; }

        [Option]
        public bool IncludeIfNotExists { get; set; }

        [Option(Hidden = false)]
        public bool IncludeHeaders { get; set; }

        [Option(HelpText = "Suppress any messages.")]
        public bool Quiet { get; set; }

    }
}