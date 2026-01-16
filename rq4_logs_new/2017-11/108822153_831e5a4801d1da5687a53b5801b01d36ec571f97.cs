using DCode.Data.ReportingRepository;
using System;
using System.Collections.Generic;
using System.Linq;

namespace DCode.Services.Reporting
{
    public class ReportingService : IReportingService
    {
        private readonly IDailyUsageStatisticsRepository _dailyUsageStatisticsRepository;

        public ReportingService(IDailyUsageStatisticsRepository dailyUsageStatisticsRepository)
        {
            _dailyUsageStatisticsRepository = dailyUsageStatisticsRepository;
        }

        public List<Tuple<DateTime, int>> GetUserVisitsCount(int noOfRecords = 1)
        {
            List<Tuple<DateTime, int>> recordsCount = null;

            for (int i = 0; i < noOfRecords; i++)
            {
                var dateTime = DateTime.Now.AddDays(-1 * i);

                var result = _dailyUsageStatisticsRepository.GetDailyStatisticsFor(dateTime);

                recordsCount = recordsCount ?? new List<Tuple<DateTime, int>>();

                recordsCount.Add(
                    Tuple.Create(
                        dateTime, 
                        result != null && result.Any() 
                            ? Convert.ToInt32(result.First().visits) 
                            : 0));
            }

            return recordsCount;
        }

        public void UpdateDailySiteVisitCount()
        {
            _dailyUsageStatisticsRepository.UpsertDailyStatistics();
        }
    }
}