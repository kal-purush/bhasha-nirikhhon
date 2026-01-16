using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NewarkProject_2020
{
    class Control_Id
    {
        //login control id
        public static string login_Click = "[href*='/Account/Login']";
        public static string username = "Email";
        public static string password = "Password";
        public static string loginbutton = "[class*='btn']";

        //register control id
        public static string register_link = "Register";
        public static string uname = "UserName";
        public static string pass = "Password";
        public static string firstname = "FirstName";
        public static string middlename = "MiddleName";
        public static string lastname = "LastName";
        public static string email = "Email";
        public static string mobile = "Mobile";
        public static string age = "Age";
        public static string genderdropdown = "Gender";
        public static string racedropdown = "Race";
        public static string fulladdress = "FullAddress";
        public static string streetnumber = "StreetNumber";
        public static string streetname = "StreetName";
        public static string city = "City";
        public static string state = "State";
        public static string zip = "Zip";
        public static string country = "Country";
        public static string educationcheckbox = "Education";
        public static string license = "License";
        public static string certification = "Certifications";
        public static string areaofinterest = "[class*='pull-left']";
        public static string createbutton = "reg_sub";


        //logout control id
        public static string logout_click = "[href*='/Home/logout']";
    }
}