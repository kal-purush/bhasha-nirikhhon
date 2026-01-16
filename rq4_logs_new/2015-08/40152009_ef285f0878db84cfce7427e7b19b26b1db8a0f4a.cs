using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DBDataAccess;
using ResultWrappers;

namespace DataAnalysis
{
	public class MethodLog
	{
		public BLResult<int> numberOfCalls()
		{
			try
			{
				var query = DBDataAccess.MethodLog
					.Query(log => log.MethodLogID > 0);

				var otherCount = query.Count();

				return new BLResult<int>(otherCount);
			}
			catch (Exception e)
			{
				return new BLResult<int>(e);
			}
		}

		public BLResult<int> numberOfCallsByDay(DateTime day)
		{
			try
			{
				DateTime start = day.AddHours(-12);
				DateTime end = day.AddHours(12);
				var query = DBDataAccess.MethodLog
					.Query(log => log.CallDate > start && log.CallDate < end);

				var otherCount = query.Count();

				return new BLResult<int>(otherCount);
			}
			catch (Exception e)
			{
				return new BLResult<int>(e);
			}
		}

		public BLResult<int[]> numberOfCalls(int daysBack)
		{
			try
			{
				var utcnow = DateTime.UtcNow;
				DateTime start = DateTime.Today.AddDays(-daysBack);
				DateTime end = utcnow;
				var query = DBDataAccess.MethodLog
					.Query(log => log.CallDate > start && log.CallDate < end);

				var list = new List<int>();

				for(int i = 0; i <= daysBack; i++)
				{
					var t_start = utcnow.AddDays(-(1+i));
					var t_end = t_start.AddDays(1);
					list.Add(query.Count(log=> 
						log.CallDate > t_start &&
						log.CallDate < t_end));
					//Console.WriteLine(String.Format("{2} {3} - {0}, {1}", i, list[i], t_start, t_end));
				}

				return new BLResult<int[]>(list.ToArray());
			}
			catch (Exception e)
			{
				return new BLResult<int[]>(e);
			}
		}
	}
}