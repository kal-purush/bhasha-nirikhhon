using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Xcaciv.Command.Core;
using Xcaciv.Command.Interface;
using Xcaciv.Command.Interface.Attributes;

namespace Xcaciv.Command.Packages
{
    [CommandRoot("Package", "Package commands")]
    [CommandRegister("Install", "install a package")]
    [CommandParameterOrdered("packagename", "The unique name of the package to install", IsRequired = true)]
    public class InstallCommand : AbstractCommand
    {
        public override string HandleExecution(string[] parameters, IEnvironmentContext env)
        {
            return "Not installing " + String.Join(',', parameters);
        }

        public override string HandlePipedChunk(string pipedChunk, string[] parameters, IEnvironmentContext env)
        {
            return $"Not installing {pipedChunk} " + String.Join(',', parameters);
        }
    }
}