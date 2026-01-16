using System;
using System.Diagnostics;
using System.Security;

namespace Common.Auditing
{
    internal static class EventLogFactory
    {
        public static EventLog CreateNew(string logName = "SBES_Replication", string sourceName = "Common.Auditing.Audit")
        {
            // TODO: Fix bug where .SourceExists throws `SecurityException`
            if (!EventLog.SourceExists(sourceName))
            {
                EventLog.CreateEventSource(sourceName, logName);
            }

            return new EventLog(logName, Environment.MachineName, sourceName);
        }
    }
}