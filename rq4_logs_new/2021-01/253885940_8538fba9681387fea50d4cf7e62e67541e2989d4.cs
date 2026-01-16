using AutoVersionsDB.Core.DBVersions.ScriptFiles;
using System.CommandLine;

namespace AutoVersionsDB.CLI.DBVersions
{
    public class DDDTargetCLIOption : Option<string>
    {
        public DDDTargetCLIOption()
            : base(new string[] { "--ddd-target", "-dt" }, $"The DevDummyData target file script name that set the db in the desired state. Set {NoneRuntimeScriptFile.TargetNoneScriptFileName} if you want to that system not run any DevDummyData script; Set {LastRuntimeScriptFile.TargetLastScriptFileName} if you want to that system not run all DevDummyData scripts; Otherwise, set the target script name.")
        {
        }
    }
}