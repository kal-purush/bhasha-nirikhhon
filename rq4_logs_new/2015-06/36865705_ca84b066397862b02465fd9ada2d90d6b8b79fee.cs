namespace IISLogTrafficGenerator.Logic.Convert
{
	using System;
	using System.Collections.Generic;
	using System.Text;
	using IISLogTrafficGenerator.Convert;
	using MSUtil;

	/*
	 * This code was adapted from IISLogToWcat, https://github.com/oogg/IISLogToWcat 
	 * 
	 */
	public class LogReader
    {
		public string logfile { get; private set; }

		public LogReader(string logfile) 
        {
			this.logfile = logfile;
        }

        public IEnumerable<LogRow> GetRequests()
        {
            var logQuery = new LogQueryClass();
            var iisInputFormat = new COMIISW3CInputContextClass();
			
			var query = @"SELECT time, cs-method, cs-uri-stem, cs-uri-query, s-port, c-ip FROM " + this.logfile;

            var set = logQuery.Execute(query, iisInputFormat);
            while (!set.atEnd())
            {
                ILogRecord record = set.getRecord();
                if (string.Concat(record.getValueEx("cs-method")).Equals("GET"))
                {
                    var path = string.Concat(record.getValueEx("cs-uri-stem"));
                    var querystring = string.Concat(record.getValueEx("cs-uri-query"));

					var stringBuilder = new StringBuilder();
                    stringBuilder.Append(path);

                    if (!String.IsNullOrWhiteSpace(querystring))
                    {
                        stringBuilder.AppendFormat("?{0}", querystring);
                    }
					string ip = string.Concat(record.getValueEx("c-ip"));
					var time = (DateTime)record.getValue("time");
                    var request = new LogRow(stringBuilder.ToString(), ip, time);
                    yield return request;
                }

                set.moveNext();
            }

            set.close();
        }
    }
}