using SchedulearnBackend.Models;
using System.Collections.Generic;
using System.Linq;

namespace SchedulearnBackend.Controllers.DTOs
{
    public class TeamMembers
    {
        public TeamMembers(Team accessibleTeam, List<User> memebersWhoLearnedTopic)
        {
            TeamId = accessibleTeam.Id;
            ManagerName = accessibleTeam.Manager.Name;
            ManagerSurname = accessibleTeam.Manager.Surname;
            Users = memebersWhoLearnedTopic.Select(u => new SimpleUser(u)).ToList();
        }

        public int TeamId { get; set; }
        public string ManagerName { get; set; }
        public string ManagerSurname { get; set; }
        public List<SimpleUser> Users { get; set; }
    }
}