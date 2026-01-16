using EyesOnTheNet.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace EyesOnTheNet.DAL
{
    public class EyesOnTheNetRepository
    {
        public EyesOnTheNetContext Context { get; set; }

        public EyesOnTheNetRepository()
        {
            Context = new EyesOnTheNetContext();

            // The following checks to see if the database exists.  If not, this makes it and returns 'true'.
            //  If the Db exists, this does nothing and returns false. I'm not using regular migrations since I'm
            //  attaching to a stand-alone MySQL server. 
            Context.Database.EnsureCreated();
        }

        public void AddFakeUser()
        {
            var user = new User
            {
                Username = "CamUser1",
                Email = "camuser1@gmail.com",
                LastLoginDate = DateTime.Now,
                RegistrationDate = DateTime.Now,
            };

            Context.Add(user);
            Context.SaveChanges();
        }
    }
}