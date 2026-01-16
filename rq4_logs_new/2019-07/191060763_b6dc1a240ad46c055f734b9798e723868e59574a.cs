using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common.Log;
using Cronos;
using Lykke.Common.Log;
using Lykke.Job.BlockchainBalancesReport.Reporting;
using Lykke.Job.BlockchainBalancesReport.Settings;

namespace Lykke.Job.BlockchainBalancesReport.Jobs
{
    public class BuildReportJob
    {
        public const string Id = "06ddbc641d9f4c26b20ae23a3956d580";

        private readonly ILog _log;
        private readonly ReportSettings _reportSettings;
        private readonly LastReportOccurrenceRepository _lastReportOccurrenceRepository;
        private readonly BalancesReportBuilder _reportBuilder;

        public BuildReportJob(
            ILogFactory logFactory,
            ReportSettings reportSettings,
            LastReportOccurrenceRepository lastReportOccurrenceRepository,
            BalancesReportBuilder reportBuilder)
        {
            _reportSettings = reportSettings;
            _lastReportOccurrenceRepository = lastReportOccurrenceRepository;
            _reportBuilder = reportBuilder;
            _log = logFactory.CreateLog(this);
        }

        public async Task ExecuteAsync(CronExpression scheduleCron)
        {
            _log.Info($"Executing the job. Schedule CRON: {scheduleCron}");

            var lastOccurrence = await _lastReportOccurrenceRepository.GetLastOccurrenceOrDefaultAsync();
            var now = DateTime.UtcNow;

            _log.Info($"Last report occurence is [{lastOccurrence:s}]");

            IReadOnlyCollection<DateTime> missedOccurrences;

            if (lastOccurrence.HasValue)
            {
                missedOccurrences = scheduleCron.GetOccurrences
                    (
                        lastOccurrence.Value,
                        now,
                        false,
                        true
                    )
                    .ToArray();
            }
            else
            {
                var gap = TimeSpan.FromMinutes(1);

                do
                {
                    var occurrences = scheduleCron.GetOccurrences
                        (
                            now - gap,
                            now,
                            false,
                            true
                        )
                        .ToArray();

                    if (occurrences.Any())
                    {
                        missedOccurrences = new[] {occurrences.Last()};
                        break;
                    }

                    gap *= 10;
                }
                while (true);
            }

            _log.Info("Missed occurrences", missedOccurrences); 

            foreach (var occurrence in missedOccurrences)
            {
                await _reportBuilder.BuildAsync(occurrence - _reportSettings.BalancesIntervalFromSchedule);
            }

            _log.Info("Executing the job is done.");
        }
    }
}