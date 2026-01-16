using System;
using System.Collections.Generic;
using System.Text;
using ProjectBank.Views;

namespace ProjectBank
{
    class Startup
    {
        public static void Run()
        {
            List<User> Users = new List<User>();
            User User1 = new User("Marre", "Cykel");
            User User2 = new User("Antonio", "Bil");
            User User3 = new User("Simon", "Buss");

            Users.Add(User1);
            Users.Add(User2);
            Users.Add(User3);


            LogInView.Run(Users);
        }
    }
}