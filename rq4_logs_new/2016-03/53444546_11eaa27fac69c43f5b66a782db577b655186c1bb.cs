using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.ComponentModel.DataAnnotations;

namespace QMaze.Models
{
    public class GameStatisticsModel
    {
        private ApplicationDbContext context = new ApplicationDbContext();

        public IEnumerable<ApplicationUser> AppUsers { get; set; }

        public GameStatisticsModel()
        {
            AppUsers = context.Users.ToList();
        }
    }
}