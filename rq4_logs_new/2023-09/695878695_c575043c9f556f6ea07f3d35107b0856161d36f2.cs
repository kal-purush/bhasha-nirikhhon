using ProjectLab.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace ProjectLab.Factory
{
    public class UserFactory
    {
        public User createUser(string username, string email, string gender, string password)
        {
            User newUser = new User();
            newUser.Username = username;
            newUser.Email = email;
            newUser.Gender = gender;
            newUser.Password = password;

            return newUser;
        }
    }
}