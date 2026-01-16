using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HelpDesk.Models
{
    public class Menu
    {

        public void MainMenu()
        {
            Console.WriteLine("Main menu");
            Console.WriteLine(" ");
            Console.WriteLine("Who is using this system?");
            Console.WriteLine("1. User");
            Console.WriteLine("2. Technician");
            Console.WriteLine("3. Exit");
            Console.WriteLine(" ");
        }

        public void User()
        {
            Console.Clear();
            Console.WriteLine("What would you like to do?");
            Console.WriteLine("1. Select User");
            Console.WriteLine("2. Create a new User");
            Console.WriteLine("3. Main Menu");


        }
        public void UserMenu()
        {
            Console.Clear();
            Console.WriteLine("What would you like to do?");
            Console.WriteLine("1. Create a new ticket");
            Console.WriteLine("2. View my tickets");
          //  Console.WriteLine("3. View resolved tickets");
            Console.WriteLine("3. Settings");
            Console.WriteLine("4. Return");
            Console.WriteLine(" ");
        }

        public void Technician()
        {
            Console.Clear();
            Console.WriteLine("1. Select a technician");
            Console.WriteLine("2. Add new technician"); 
            Console.WriteLine("3. Return");
            Console.WriteLine(" ");
        }

        public void TechMenu()
        {
            Console.Clear();
            Console.WriteLine("1. View my tickets");
            Console.WriteLine("2. View open tickets");
            //    Console.WriteLine("3. Add new technician");
            Console.WriteLine("3. Settings");
            Console.WriteLine("4. Return");
            Console.WriteLine(" ");
        }

        public void TicketMenu()
        {
            Console.WriteLine("What would you like to do?");
            Console.WriteLine("1. Add a response ");
            Console.WriteLine("2. Mark ticket resolved");
            Console.WriteLine("3. Reassign ticket to a technician");
            Console.WriteLine("4. Return");

            Console.WriteLine(" ");
        }

        public void TicketSettings()
        {
            Console.WriteLine("");
            System.Console.WriteLine("1. Update User Account");
            System.Console.WriteLine("2. Delete Ticket");
            System.Console.WriteLine("3. Delete User Account");
            System.Console.WriteLine("4. Return");
        }

        public void SettingsMenu()
        {
            Console.WriteLine("");
            System.Console.WriteLine("1. Update Bio");
         //   System.Console.WriteLine("2. Update Technican ID");
            System.Console.WriteLine("2. Delete Technician");
            System.Console.WriteLine("3. Return");
        }

    }
}