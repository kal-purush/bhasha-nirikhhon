using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.Management.Smo;

namespace SqlDataScripter
{
    class TableScripter
    {
        private readonly Scripter scripter;
        private readonly Server server;
        private readonly TextWriter output;

        public ScriptingOptions Options { get => scripter.Options; set => scripter.Options = value; }

        public TableScripter(Server svr, TextWriter output)
        {
            server = svr;
            this.output = output;
            scripter = new Scripter(svr);
            scripter.DiscoveryProgress += Scripter_DiscoveryProgress;
            scripter.ScriptingProgress += Scripter_ScriptingProgress;
        }

        private void Scripter_ScriptingProgress(object sender, ProgressReportEventArgs e)
        {
            output?.WriteLine("Scripting table {0}", (server.GetSmoObject(e.Current) as NamedSmoObject)?.Name ?? "UNKNOWN");
        }

        private void Scripter_DiscoveryProgress(object sender, ProgressReportEventArgs e)
        {
            output?.WriteLine("Discovering object {0}", (server.GetSmoObject(e.Current) as NamedSmoObject)?.Name ?? "UNKNOWN");
        }

        public IEnumerable<string> ScriptTables(params SqlSmoObject[] objects)
        {
            return scripter.EnumScript(objects);
        }

    }
}