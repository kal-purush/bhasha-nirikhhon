using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DiagnosticAdapter;
using System;
using System.Diagnostics;
using System.IO;
using System.Threading;

namespace PerfDiagnosticListener
{
    public class PerfDiagnosticListener
    {
        private static long _requestsBegan = 0;
        private static long _requestsEnded = 0;
        private static long _requestTicks = 0;

        [DiagnosticName("Microsoft.AspNetCore.Hosting.BeginRequest")]
        public void OnBeginRequest(HttpContext httpContext, long timestamp)
        {
            Interlocked.Increment(ref _requestsBegan);

            var requestInfo = new RequestInfo { BeginTimestamp = timestamp };
            httpContext.Features.Set(requestInfo);
        }

        [DiagnosticName("Microsoft.AspNetCore.Hosting.EndRequest")]
        public void OnEndRequest(HttpContext httpContext, long timestamp)
        {
            Interlocked.Increment(ref _requestsEnded);

            var requestInfo = httpContext.Features.Get<RequestInfo>();
            Interlocked.Add(ref _requestTicks, timestamp - requestInfo.BeginTimestamp);
        }

        public static void Write(TextWriter writer)
        {
            var currentRequests = _requestsBegan - _requestsEnded;
            var secondsPerRequest = (((double)_requestTicks) / _requestsEnded) / Stopwatch.Frequency;

            writer.WriteLine(
                $"[{DateTime.Now.ToString("HH:mm:ss.fff")}] " +
                $"CurrentRequests: {currentRequests}\t" +
                $"RequestsEnded: {_requestsEnded}\t" +
                $"SecondsPerRequest: {Math.Round(secondsPerRequest, 2)}"
            );
        }

        private class RequestInfo
        {
            public long BeginTimestamp { get; set; }
        }
    }
}