using System;
using System.Collections.Generic;
using System.Text;

namespace ProjectBank
{
    class Login : User
    {
        private new string Username = "Marre";
        private new string Password = "Cykel";

        public int LoginAttempts;
        public bool IsLocked;
        public void Logint()
        {
            string text = @"
              /_\  _ __   __ _ _ __   __ _ ___   / __\ __ _ _ __ | | __
             //_\\| '_ \ / _` | '_ \ / _` / __| /__\/// _` | '_ \| |/ /
            /  _  \ | | | (_| | | | | (_| \__ \/ \/  \ (_| | | | |   < 
            \_/ \_/_| |_|\__,_|_| |_|\__,_|___/\_____/\__,_|_| |_|_|\_\";
            Console.WriteLine(text);


            if (Username == "Marre" & Password == "Cykel")
            {
                IsLocked = false;
                Console.WriteLine("Welcome to your dashboard");
            }
            else
                Console.WriteLine("Wrong username or password. Please try again");
            IsLocked = true;
            LoginAttempts--;
            if (LoginAttempts == 0)
            {
                Console.WriteLine("You have attempt to many times. Your account is now locked. Please contact our costomer service");
            }
        }
    }
}