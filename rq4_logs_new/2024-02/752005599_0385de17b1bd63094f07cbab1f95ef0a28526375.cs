using System;

namespace JiraWorkLogsService
{
    static class ServiceConstants
    {
        public static string Jql
        {
            get
            {
                var jql = Environment.GetEnvironmentVariable("JIRA_JQL") ?? "";

                if (string.IsNullOrWhiteSpace(jql))
                {
                    var previousMonth = DateTime.Now.AddMonths(-1);
                    var start = new DateTime(previousMonth.Year, previousMonth.Month, 1);
                    var finish = start.AddMonths(1);

                    jql = $"worklogDate >= '{start:yyyy-M-d}' AND worklogDate < '{finish:yyyy-M-d}'";
                }

                return jql;
            }
        }
    }
}