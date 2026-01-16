using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace commerce.Models
{
    public class users
{

        public string Email;
        public string password;

        public static bool log(string Email, string password)
        {
            if (Email == "admin" && password == "admin")
                return true;
            else
                return false;


        }






    }
}