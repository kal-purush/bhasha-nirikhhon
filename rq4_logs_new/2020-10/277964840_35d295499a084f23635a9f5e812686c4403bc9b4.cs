using F1Telemetry.Core.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace F1Telemetry.Core.Util.Extensions
{
    public static class BasePacketDataExtensions
    {
        public static IEnumerable<T> GetForLap<T>(this IList<T> data, IEnumerable<LapData> lapData) where T : BasePacketData
        {
            var matchingLapData = new List<T>();
            var dataCopy = data.ToArray();

            foreach (var lap in lapData)
            {
                var lData = dataCopy.SingleOrDefault(c => c.SessionTime.Equals(lap.SessionTime) && c.SessionUID.Equals(lap.SessionUID));

                if (lData != null)
                {
                    matchingLapData.Add(lData);
                }
            }

            return matchingLapData;
        }
    }
}