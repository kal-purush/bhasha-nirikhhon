using System;
using System.Diagnostics;

namespace Common.Auditing
{
    public class Audit : IDisposable
    {
        private static EventLog customLog;
        private const string SourceName = "Common.Auditing.Audit";
        private const string LogName = "SBES_Replication";

        static Audit()
        {
            try
            {
                if (!EventLog.SourceExists(SourceName))
                {
                    EventLog.CreateEventSource(SourceName, LogName);
                }

                customLog = new EventLog(LogName, Environment.MachineName, SourceName);
            }
            catch (Exception e)
            {
                customLog = null;
                Console.WriteLine("Error while trying to create log handle. Error = {0}", e.Message);
            }
        }

        public static void ReplicationSuccess(Alarm alarm)
        {
            if (customLog != null)
            {
                string message = string.Format(AuditEvents.AlarmReplicationSuccess, alarm);
                customLog.WriteEntry(message, EventLogEntryType.Information);
            }
            else
            {
                throw new ArgumentException(string.Format("Error while trying to write event (eventid = {0}) to event log.", (int)AuditEventTypes.AlarmReplicationSuccess));
            }
        }

        public static void ReplicationFailure(Alarm alarm, string reason = "")
        {
            if (customLog != null)
            {
                string message = string.Format(AuditEvents.AlarmReplicationFailure, alarm, reason);
                customLog.WriteEntry(message, EventLogEntryType.Error);
            }
            else
            {
                throw new ArgumentException(string.Format("Error while trying to write event (eventid = {0}) to event log.", (int)AuditEventTypes.AlarmReplicationFailure));
            }
        }

        public static void ReplicationInitiated()
        {
            if (customLog != null)
            {
                string message = string.Format(AuditEvents.AlarmReplicationInitiated, DateTime.Now);
                customLog.WriteEntry(message, EventLogEntryType.Information);
            }
            else
            {
                throw new ArgumentException(string.Format("Error while trying to write event (eventid = {0}) to event log.", (int)AuditEventTypes.AlarmReplicationInitiated));
            }
        }

        public void Dispose()
        {
            if (customLog != null)
            {
                customLog.Dispose();
                customLog = null;
            }
        }
    }
}