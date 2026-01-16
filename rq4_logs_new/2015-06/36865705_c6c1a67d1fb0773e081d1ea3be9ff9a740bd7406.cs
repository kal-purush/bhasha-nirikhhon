namespace IISLogTrafficGenerator.Logic
{
	using System;
	using System.Collections.Generic;
	using System.IO;
	using System.Text;

	public class LogReader
    {
		private StreamReader logfileStream;

		private string logfile = string.Empty;

		private const string FIELDSSTRING = "#Fields:";

		public LogReader(string logfile)
		{
			this.logfile = logfile;
			this.LogfileStream = new StreamReader(logfile);
		}

		public LogReader(Stream logfileStream)
		{
			this.logfileStream = new StreamReader(logfileStream);
		}

		public StreamReader LogfileStream
		{
			get
			{
				return this.logfileStream;
			}
			private set
			{
				this.logfileStream = value;
			}
		}

		public IEnumerable<LogRow> ParseLogFile()
		{
			List<LogRow> list = new List<LogRow>();

			if (this.logfileStream == null) throw new ApplicationException("Unable to open log file");

			int dateidx = -1;
			int timeIdx = -1;
			int methodIdx = -1;
			int uriIdx = -1;
			int queryIdx = -1;
			int portIdx = -1;
			
			while (!this.logfileStream.EndOfStream)
			{
				var row = string.Concat(this.logfileStream.ReadLine());
				if (row.StartsWith("#"))
				{
					if (row.StartsWith(FIELDSSTRING))
					{
						var fields = row.Substring(FIELDSSTRING.Length+1).Split(' ');

						dateidx = Array.FindIndex(fields, p => p.ToLower().Equals("date"));
						timeIdx = Array.FindIndex(fields, p => p.ToLower().Equals("time"));
						methodIdx = Array.FindIndex(fields, p => p.ToLower().Equals("cs-method"));
						uriIdx = Array.FindIndex(fields, p => p.ToLower().Equals("cs-uri-stem"));
						queryIdx = Array.FindIndex(fields, p => p.ToLower().Equals("cs-uri-query"));
						portIdx = Array.FindIndex(fields, p => p.ToLower().Equals("s-port"));
					}
				}
				else
				{
					var method = this.GetValue(methodIdx, row, "");
					if (!method.Equals("GET")) continue;

					var datestr = this.GetValue(dateidx, row, DateTime.Now.ToShortDateString());
					var timestr = this.GetValue(timeIdx, row, "00:00:00");
					DateTime time;

					DateTime.TryParse(string.Concat(datestr, " ", timestr), out time);
					
					var path = this.GetValue(uriIdx, row, "");
					var querystring = this.GetValue(queryIdx, row, "/");
					var port = this.GetValue(portIdx, row, "80");

					var stringBuilder = new StringBuilder();
					stringBuilder.Append(path);

					if (!String.IsNullOrWhiteSpace(querystring) && !querystring.Equals("-"))
					{
						stringBuilder.AppendFormat("?{0}", querystring);
					}
					var logrow = new LogRow(stringBuilder.ToString(), port, time);
					//list.Add(logrow);
					yield return logrow;
				}
			}

			this.logfileStream.Close();
		}

		public string GetValue(int idx, string row, string defaultValue)
		{
			var strings = row.Split(' ');
			var retval = defaultValue;

			if (idx > -1 && idx < strings.Length )
			{
				retval = strings[idx];
			}

			return retval;
		}
    }
}