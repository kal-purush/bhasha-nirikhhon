using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp1
{
    public class PerformanceMonitor
    {
        private Stack<MethodCall> _callStack = new Stack<MethodCall>();
        private Dictionary<string, MethodCallStatistic> _callsStats = new Dictionary<string, MethodCallStatistic>();
        private Stopwatch _stopwatch = new Stopwatch();

        /// <summary>
        /// Can call it like this: perfMon.StartCall(GetCaller());
        /// </summary>
        public void StartCall(string methodName)
        {
            if (_callStack.Count == 0)
                _stopwatch.Start();

            _callStack.Push(new MethodCall { MethodName = methodName, StartTimeMillisec = _stopwatch.ElapsedMilliseconds });
        }

        // Get name of current method
        //public static string GetCaller([CallerMemberName] string caller = null)
        //{
        //    return caller;
        //}

        public void EndCall()
        {
            if (_callStack.Count == 0)
                throw new Exception("Mismatched Start/End calls");

            MethodCall call = _callStack.Pop();
            long callTime = _stopwatch.ElapsedMilliseconds - call.StartTimeMillisec;
            call.TotalTimeMillisec += callTime;

            foreach (MethodCall callInStack in _callStack)
            {
                callInStack.TotalTimeMillisec -= call.TotalTimeMillisec;
            }

            AddStatistic(call);

            if (_callStack.Count == 0)
                DisplayStats();
        }

        private void DisplayStats()
        {
            List<MethodCallStatistic> statsList = _callsStats.Select(p => p.Value).OrderByDescending(s => s.TotalTimeMillisec).ToList();

            foreach (MethodCallStatistic stat in statsList)
            {
                Debug.Print($"{stat.MethodName}: {stat.TotalTimeMillisec}ms ({stat.CallCount})");
            }
        }

        private void AddStatistic(MethodCall call)
        {
            bool found = _callsStats.TryGetValue(call.MethodName, out MethodCallStatistic stat);

            if (found == false)
            {
                stat = new MethodCallStatistic();
                stat.MethodName = call.MethodName;
                _callsStats.Add(call.MethodName, stat);
            }

            stat.CallCount++;
            stat.TotalTimeMillisec += call.TotalTimeMillisec;
        }

        class MethodCall
        {
            public string MethodName { get; set; }
            public long StartTimeMillisec { get; set; }
            public long TotalTimeMillisec { get; set; }
        }

        class MethodCallStatistic
        {
            public string MethodName { get; set; }
            public long TotalTimeMillisec { get; set; }
            public int CallCount { get; set; }
        }
    }
}