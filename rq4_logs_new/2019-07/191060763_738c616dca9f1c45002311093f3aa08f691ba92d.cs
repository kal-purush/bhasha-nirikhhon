using System;
using System.Collections.Generic;
using System.Linq;
using Cronos;

namespace Lykke.Job.BlockchainBalancesReport.Services
{
    public class JobMissedOccurrencesProvider
    {
        public IReadOnlyCollection<DateTime> GetMissedOccurrenceAsync(CronExpression scheduleCron, DateTime? lastOccurrence, DateTime now)
        {
            if (lastOccurrence.HasValue)
            {
                return scheduleCron.GetOccurrences
                    (
                        lastOccurrence.Value,
                        now,
                        false,
                        true
                    )
                    .ToArray();
            }

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
                    return new[] {occurrences.Last()};
                }

                gap *= 10;
            }
            while (true);
        }
    }
}