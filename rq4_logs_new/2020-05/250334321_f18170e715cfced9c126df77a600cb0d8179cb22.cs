using SchedulearnBackend.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SchedulearnBackend.Controllers.DTOs
{
    public class TeamTopics
    {
        public int TeamId { get; set; }
        public string ManagerName { get; set; }
        public string ManagerSurname { get; set; }
        public List<TopicNoSubtopics> Topics { get; set; }

        public TeamTopics(Team team, List<Topic> topics)
        {
            TeamId = team.Id;
            ManagerName = team.Manager.Name;
            ManagerSurname = team.Manager.Surname;
            Topics = topics.Select(t => new TopicNoSubtopics(t)).ToList();
        }
    }
}