using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace ODWai2.ODWaiCore
{
    public class CommandBuilder
    {
        public enum ExecutionType
        {
            main = 0,
            util = 1
        }

        private string _base_path = Path.GetFullPath("../../../../");

        public CommandBuilder() {}

        public static CommandBuilder shared()
        {
            return new CommandBuilder();
        }

        public string command(ExecutionType type, string script_name, params (string, string)[] arguments)
        {
            string script_path = _base_path.Replace("\\", "/") + sub_path(type) + script_name;
            if (!File.Exists(script_path)) return null;

            string command = script_path;
            foreach ((string key, string value) pair in arguments)
            {
                command += (" --" + pair.key + " " + pair.value);
            }

            return command;
        }

        private string sub_path(ExecutionType type)
        {
            switch (type)
            {
                case ExecutionType.main:
                    return "scripts/";
                case ExecutionType.util:
                    return "utilities/";
                default:
                    return "";
            }
        }
    }
}