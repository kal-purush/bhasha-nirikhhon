using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;

namespace PluginApp
{
    internal class PluginHelper
    {
        private static readonly IEnumerable<(string, string, string, string[])> pluginInfo = new List<(string, string, string, string[])>
        {
            ("2.0.0.0", "ICommand", "Execute", Array.Empty<string>()),
            ("2.2.0.0", "ICommand", "ExecuteAsync", new[] { "name", "version" }),
        };

        internal const string PluginInterfaceName = "ICommand";

        internal static Func<Assembly, bool> GetPluginType = assembly
            =>
            {
                var assemblyInfo = pluginInfo.FirstOrDefault(x => x.Item1 == assembly.GetName().Version.ToString());

                return assembly
                    .GetTypes()
                    .Any(x => x.Name == assemblyInfo.Item2);
            };

        internal static MethodInfo GetExecutor(Type type)
        {
            if (type.Assembly.GetName()?.Version is null)
                return null;

            MethodInfo executorMethod = GetMethodBasedOnVersion(type, type.Assembly.GetName().Version.ToString());

            if (executorMethod is null)
                throw new InvalidDataContractException("Contract not implemented.");

            return executorMethod;
        }

        internal static object[] GetParameterInput(string version, IEnumerable<(string, string)> parameterInfos)
            => version switch
            {
                "2.2.0.0" => GetParameters_v2_2(version, parameterInfos),
                _ => null,
            };

        private static object[] GetParameters_v2_2(string version, IEnumerable<(string, string)> pInfos)
        {
            var parameterNames = pluginInfo.First(x => x.Item1 == version).Item4;
            var pInfoNames = pInfos.Select(x => x.Item1);

            var isParameterMatch = parameterNames.All(x => pInfoNames.Contains(x));
            return isParameterMatch
                ? pInfos.Select(x => x.Item2).Cast<object>().ToArray()
                : null;
        }

        private static MethodInfo GetMethodBasedOnVersion(Type type, string version)
            => version switch
            {
                "2.0.0.0" => type.GetMethod(pluginInfo.FirstOrDefault(x => x.Item1 == version).Item3),
                "2.2.0.0" => type.GetMethod(pluginInfo.FirstOrDefault(x => x.Item1 == version).Item3),
                _ => null,
            };
    }
}