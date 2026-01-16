using DataGeneration.Maint;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace DataGeneration
{
    public class ConsoleExecutor
    {
        #region Args

        public static class Args
        {
            public const string Config = "/config";
            public const string Start = "/start";
            public const string UpdateEndpoint = "/update-endpoint";
            public const string SaveEndpoint = "/save-endpoint";
            public const string Help = "/?";

            public static readonly Dictionary<string, string> AvailableArgsHelp = new Dictionary<string, string>
            {
                [Start] = "Start generation. (Default - \"true\" if no args specified, \"false\" if any arg is specified)",
                [UpdateEndpoint] = "Update endpoint. If value not specified \"datagen-endpoint.xml\" file will be used.",
                [SaveEndpoint] = "Save endpoint. Values should be in following order {version} {endpoint} {filepath}",
                [Config] = "Specify config file. If not specified default will be used.",
                [Help] = "Show this help."
            };
        }

        public const string GlobalHelp = "This tool for generation data via Contact-Based API to Acumatica. \r\n" +
            "Also you can use additional options presented bellow.";

        #endregion

        #region Console

        public const string ConfigFileName = "config.json";
        public const string EndpointDatagenFileName = "endpoint-datagen.xml";

        private Lazy<GeneratorConfig> _config = new Lazy<GeneratorConfig>(() => GeneratorConfig.ReadConfig(ConfigFileName));
        public GeneratorConfig Config => _config.Value;

        public static void WriteInfo(string line, ConsoleColor color = ConsoleColor.Gray, Exception e = null)
        {
            Console.ForegroundColor = color;

            string getLine(string line_) => '|' + line_ + '|';

            var limitLine = getLine(new string('-', line.Length));

            Console.WriteLine();
            Console.WriteLine(limitLine);
            Console.WriteLine(getLine(line));
            Console.WriteLine(limitLine);
            Console.WriteLine();
            if (e != null)
                Console.WriteLine(e);

            Console.ForegroundColor = ConsoleColor.Gray;
        }

        /// <summary>
        ///     Execute args (if any) and return value indicates to start datageneration or not (depending on args).
        /// </summary>
        /// <param name="args"></param>
        /// <returns>Specifies to start datageneration.</returns>
        public bool ExecuteArgs(IEnumerable<string> args)
        {
            var executer = new ArgsExecutor(args);
            if (executer.Empty())
                return false;

            try
            {
                var start = false;
                executer
                    .Arg(Args.Help, ShowHelp, stopOnSuccess: true)
                    .Arg_ThrowNoValue(Args.Config, value => _config = new Lazy<GeneratorConfig>(() => GeneratorConfig.ReadConfig(value)))
                    .Arg(Args.UpdateEndpoint, PutEndpoint, () => PutEndpoint(EndpointDatagenFileName))
                    .Arg_ThrowNoValues(Args.SaveEndpoint, GetAndSaveEndpoint)
                    .Arg(Args.Start, () => start = true)
                    .Default(WrongArgumentsSpecified);
                return start;
            }
            catch (ArgsExecutionException aee)
            {
                WriteInfo(aee.Message, ConsoleColor.Red, aee);
                return false;
            }
            catch (Exception e)
            {
                WriteInfo("Unexpected exception occurred.", ConsoleColor.Red, e);
                return false;
            }
        }

        private void ShowHelp()
        {
            WriteInfo("Help", ConsoleColor.Green);
            Console.WriteLine(GlobalHelp);
            Console.WriteLine();
            Console.WriteLine("Args:");
            Console.WriteLine();
            const int maxChars = 24;
            const int startSpacesCount = 4;
            var startSpaces = new string(' ', startSpacesCount);
            foreach (var arg in Args.AvailableArgsHelp)
            {
                var spacesCount = maxChars - arg.Key.Length - startSpacesCount;
                if (spacesCount <= 0)
                    spacesCount = 1;
                var spaces = new string(' ', spacesCount);
                Console.WriteLine(startSpaces + arg.Key + spaces + ":  " + arg.Value);
            }
        }

        private void WrongArgumentsSpecified()
        {
            WriteInfo("Wrong arguments specified.", ConsoleColor.Red);
            ShowHelp();
        }

        #endregion

        #region Execution

        public void PutEndpoint(string file)
        {
            if (file == null) throw new ArgumentNullException(nameof(file));
            if (!File.Exists(file)) throw new FileNotFoundException(nameof(file));

            using (var maint = MaintenanceClient.LoginLogoutClient(Config.ApiConnectionConfig))
            {
                maint.PutFileSchema(file);
            }

            Console.WriteLine($"Endpoint successfully updated from file {file}.");
        }

        private void GetAndSaveEndpoint(string[] args)
        {
            if (args.Length < 3)
                throw new InvalidOperationException("Not all arguments specified.");
            GetAndSaveEndpoint(args[0], args[1], args[2]);
        }
        public void GetAndSaveEndpoint(string version, string endpoint, string file)
        {
            if (version == null) throw new ArgumentNullException(nameof(version));
            if (endpoint == null) throw new ArgumentNullException(nameof(endpoint));
            if (file == null) throw new ArgumentNullException(nameof(file));

            using (var maint = MaintenanceClient.LoginLogoutClient(Config.ApiConnectionConfig))
            {
                maint.GetAndSaveSchema(version, endpoint, file);
            }

            Console.WriteLine($"Endpoint {endpoint}/{version} successfully saved to file {file}.");
        }

        #endregion

        #region ArgsExecutor

        private class ArgsExecutor
        {
            private readonly Dictionary<string, string[]> _args;
            private bool _any;
            private bool _stopped;

            public ArgsExecutor(IEnumerable<string> args)
            {
                string key = null;
                _args = args?.Where(a => !string.IsNullOrWhiteSpace(a))
                    .Select(a =>
                    {
                        var arg = a.Trim();
                        string value = null;
                        if (arg.StartsWith("/"))
                        {
                            key = arg;
                        }
                        else
                        {
                            value = arg;
                        }
                        return new { key, value };
                    })
                    .GroupBy(a => a.key)
                    .ToDictionary(
                        a => a.Key,
                        a =>
                        {
                            var vals = a
                                .Select(aa => aa.value)
                                .Where(aa => aa != null)
                                .ToArray();
                            if (!vals.Any())
                                return null;
                            return vals;
                        });
            }

            // if some option caught
            public bool Any() => _any;

            public ArgsExecutor Arg(string arg, Action action, bool stopOnSuccess = false)
            {
                return Arg(arg, (Action<string[]>) null, action, stopOnSuccess);
            }

            // default action only if value is null
            public ArgsExecutor Arg(string arg, Action<string> action, Action defaultAction = null, bool stopOnSuccess = false)
            {
                return Arg(arg, (string[] s) => action(s[0]), defaultAction, stopOnSuccess);
            }

            public ArgsExecutor Arg_ThrowNoValue(string arg, Action<string> action, bool stopOnSuccess = false)
            {
                return Arg_ThrowNoValues(arg, (string[] s) => action(s[0]), stopOnSuccess);
            }

            public ArgsExecutor Arg(string arg, Action<string[]> action, Action defaultAction = null, bool stopOnSuccess = false)
            {
                if (_stopped) return this;

                if(action == null && defaultAction == null)
                    throw new InvalidOperationException("Both action and defaultAction cannot be null.");

                if (_args.TryGetValue(arg, out var value))
                {
                    if (value != null)
                    {
                        try
                        {
                            if (action != null)
                                action(value);
                            else
                                defaultAction();
                        }
                        catch (ArgsExecutionException) { throw; }
                        catch(Exception e)
                        {
                            throw new ArgsExecutionException($"Execution \"{arg}\" with values failed.", e);
                        }
                        _any = true;
                    }
                    else if (defaultAction != null)
                    {
                        try
                        {
                            defaultAction();
                        }
                        catch (ArgsExecutionException) { throw; }
                        catch (Exception e)
                        {
                            throw new ArgsExecutionException($"Execution \"{arg}\" without values failed.", e);
                        }
                        _any = true;
                    }
                    _stopped |= stopOnSuccess;
                }
                return this;
            }

            public ArgsExecutor Arg_ThrowNoValues(string arg, Action<string[]> action, bool stopOnSuccess = false)
            {
                return Arg(arg, action, () => throw new ArgsExecutionException($"No value specified for option \"{arg}\"."), stopOnSuccess);
            }

            public ArgsExecutor Default(Action action)
            {
                if (!_stopped && !_any)
                    action();
                return this;
            }

            // if no options
            public bool Empty() => _args == null || !_args.Any();
        }

        private class ArgsExecutionException : Exception
        {
            public ArgsExecutionException(string message) : base(message)
            {

            }
            public ArgsExecutionException(string message, Exception innerException) : base(message, innerException)
            {

            }
        }
        #endregion
    }
}