using System;
using System.Collections.Generic;
using System.Text;

namespace ProjectBank.Views
{
    class AdminCustomerView
    {
        public static void Run(List<User> users)
        {
            string userList = "";
            foreach (var item in users)
            {
                userList += $"{item.Username}\n" +
                               $"-------------------------------\n";
            }

            string[] options = { "Exit" };
            AdminView viewUI = new AdminView(userList, options);
            int selectedOption = viewUI.Run();
            switch (selectedOption)
            {
                case 0:
                    AdminDashboard.Run(users);
                    break;
            }
        }
    }
}