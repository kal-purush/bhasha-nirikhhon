using SchedulearnBackend.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SchedulearnBackend.Controllers.DTOs
{
    public class FlatTeam
    {
        public int TeamId { get; set; }
        public int ManagerId { get; set; }
        public string ManagerName { get; set; }
        public string ManagerSurname { get; set; }

        public FlatTeam(Team accessibleTeam)
        {
            TeamId = accessibleTeam.Id;
            ManagerId = accessibleTeam.Manager.Id;
            ManagerName = accessibleTeam.Manager.Name;
            ManagerSurname = accessibleTeam.Manager.Surname;
        }
    }
}