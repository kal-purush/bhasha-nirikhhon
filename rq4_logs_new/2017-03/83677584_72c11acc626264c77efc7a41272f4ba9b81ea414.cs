using System;
using System.Runtime.CompilerServices;

namespace KpdApps.Common.MsCrm2015.Extensions
{
    public static class PluginStateTraceExtensions
    {
        public static void Trace(this PluginState pluginState, string format, params object[] args)
        {
            pluginState.TracingService.Trace(format, args);
        }

        public static void TraceError(this PluginState pluginState, string message,
            [CallerMemberName] string memberName = "",
            [CallerFilePath] string sourceFilePath = "",
            [CallerLineNumber] int sourceLineNumber = 0)
        {
            pluginState.Trace($"[Error] {message}\r\n   At MemberName: {memberName}:Ln {sourceLineNumber}\r\n   At SourceFilePath: {sourceFilePath}\r\n");
        }

        public static void TraceError(this PluginState pluginState, Exception exception)
        {
            pluginState.Trace($"[Error] {exception.Source}:{exception.Message}\r\n{exception.StackTrace}\r\n");
            if (exception.InnerException != null)
                pluginState.TraceError(exception.InnerException);
        }

        public static void TraceWarning(this PluginState pluginState, string format, params object[] args)
        {
            pluginState.Trace($"[Warning] {string.Format(format, args)}");
        }

        public static void TraceInfo(this PluginState pluginState, string format, params object[] args)
        {
            pluginState.Trace($"[Info] {string.Format(format, args)}");
        }
    }
}