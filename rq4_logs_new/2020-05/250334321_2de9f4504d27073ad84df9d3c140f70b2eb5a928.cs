using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using SchedulearnBackend.Models;

namespace SchedulearnBackend.Controllers.DTOs
{
    public class LimitToCreate
    {
        public string Name { get; set; }
        public int LimitOfConsecutiveLearningDays { get; set; }
        public int LimitOfLearningDaysPerMonth { get; set; }
        public int LimitOfLearningDaysPerQuarter { get; set; }
        public int LimitOfLearningDaysPerYear { get; set; }

        public Limit CreateLimit()
        {
            return new Limit()
            {
                Name = Name,
                LimitOfConsecutiveLearningDays = LimitOfConsecutiveLearningDays,
                LimitOfLearningDaysPerMonth = LimitOfLearningDaysPerMonth,
                LimitOfLearningDaysPerQuarter = LimitOfLearningDaysPerQuarter,
                LimitOfLearningDaysPerYear = LimitOfLearningDaysPerYear,
            };
        }
    }
}