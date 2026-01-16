using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace QMaze.Models
{
   
    public class UserStatisticsModel
    {
        private ApplicationDbContext context = new ApplicationDbContext();

        private string Id;

        public UserStatisticsModel(string Id)
        {
            this.Id = Id;
        }

        public ApplicationUser AppUser
        {
            get
            {
                return context.Users.Where(x => x.Id == this.Id).First();
            }
        }
    }
}