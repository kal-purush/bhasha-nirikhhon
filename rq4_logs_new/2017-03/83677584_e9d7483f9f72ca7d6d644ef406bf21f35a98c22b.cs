using System;
using System.Runtime.CompilerServices;
using Microsoft.Xrm.Sdk;

namespace KpdApps.Common.MsCrm2015.Extensions
{
    public static class TracingServiceExtensions
    {
        public static void TraceError(this ITracingService tracingService, string message,
            [CallerMemberName] string memberName = "",
            [CallerFilePath] string sourceFilePath = "",
            [CallerLineNumber] int sourceLineNumber = 0)
        {
            tracingService.Trace($"[Error] {message}\r\n   At MemberName: {memberName}:Ln {sourceLineNumber}\r\n   At SourceFilePath: {sourceFilePath}\r\n");
        }

        public static void TraceError(this ITracingService tracingService, Exception exception)
        {
            tracingService.Trace($"[Error] {exception.Source}:{exception.Message}\r\n{exception.StackTrace}\r\n");
            if (exception.InnerException != null)
                tracingService.TraceError(exception.InnerException);
        }

        public static void TraceWarning(this ITracingService tracingService, string format, params object[] args)
        {
            tracingService.Trace($"[Warning] {string.Format(format, args)}");
        }

        public static void TraceInfo(this ITracingService tracingService, string format, params object[] args)
        {
            tracingService.Trace($"[Info] {string.Format(format, args)}");
        }
    }
}