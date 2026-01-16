using SchedulearnBackend.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SchedulearnBackend.Controllers.DTOs
{
    public class CreateNewLearningDay
    {
        public int UserId { get; set; }
        public int TopicId { get; set; }
        public DateTime DateFrom { get; set; }
        public DateTime DateTo { get; set; }
        public string Description { get; set; }

        public LearningDay CreateLearningDay() 
        {
            return new LearningDay()
            {
                UserId = UserId,
                TopicId = TopicId,
                DateFrom = DateFrom,
                DateTo = DateTo,
                Description = Description
            };
        }
    }
}