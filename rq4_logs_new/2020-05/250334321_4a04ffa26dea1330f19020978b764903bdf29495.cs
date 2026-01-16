using SchedulearnBackend.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SchedulearnBackend.Controllers.DTOs
{
    public class CreateNewTopic
    {
        public string Name { get; set; }
        public string Description { get; set; }
        public int ParentTopicId { get; set; }

        public Topic CreateTopic() 
        {
            return new Topic()
            {
                Name = Name,
                Description = Description,
                ParentTopicId = ParentTopicId
            };
        }
    }
}