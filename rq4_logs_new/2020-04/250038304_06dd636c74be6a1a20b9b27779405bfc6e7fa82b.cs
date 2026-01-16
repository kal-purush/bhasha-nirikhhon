using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace ODWai2.ODWaiCore.Controllers
{
    class ODWaiSimulator
    {
        private static string RESULT_PATH = @"../../temp result/temp.json";

        // TODO: add input set path
        public static (int, string) start_simulation(Action start, Action completion)
        {
            if (!File.Exists(RESULT_PATH)) { return (69, null); }
            start?.Invoke();
            string result_path = Helper.get_path_argument(Path.GetFullPath(RESULT_PATH));
            (int code, string output) = ScriptExecutor.python_execute(CommandBuilder.ExecutionType.main,
                                                                      "odwai_simulator.py", false,
                                                                      completion, 60, false,
                                                                      ("result-path", result_path));
            completion?.Invoke();
            switch (code)
            {
                case 68:
                case 69:
                case 0:
                    return (code, null);
                default:
                    return (61, output);
            }
        }
    }
}