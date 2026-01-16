using System;
using System.Collections.Generic;
using System.Linq;
using SS.Api.models;
using SS.Db.models.scheduling;

namespace SS.Api.helpers.extensions
{
    public static class ShiftExtensions
    {
        public static List<ShiftBucket> GetShiftBuckets(this List<Shift> shifts)
        {
            var shiftBuckets = new List<ShiftBucket>();
            if (!shifts.Any())
                return shiftBuckets;

            var shiftsByStartDate = shifts.OrderBy(s => s.StartDate).ToList();
            var shiftBucket = new ShiftBucket { Start = shiftsByStartDate!.First().StartDate, Timezone = shifts.First().Timezone };
            Shift previousShift = null;
            foreach (var shift in shiftsByStartDate)
            {
                if (previousShift != null && previousShift.EndDate != shift.StartDate)
                {
                    shiftBuckets.Add(shiftBucket);
                    shiftBucket = new ShiftBucket {Start = shift.StartDate, Timezone = shifts.First().Timezone};
                }
       
                previousShift = shift;
                shiftBucket.End = shift.EndDate;
            }
            shiftBuckets.Add(shiftBucket);

            return shiftBuckets;
        }
    }
}