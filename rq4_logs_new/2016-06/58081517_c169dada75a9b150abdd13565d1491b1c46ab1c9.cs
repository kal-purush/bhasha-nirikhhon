using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ReviewProject.SecondReview
{
    public class IPhone
    {
        private string phoneNumber;
        private int age;
        private double speed;
        private long regNumber;
        private bool didItRain;
        private static int phoneId;
        private const string NAME = "IPhone 6Plus";
       
        // A local variable is a variable which its scope is limited to a method
        // There are two kinds of local variables - within method local variables and parameter local variables
        // Local Variable cannot be used without being initialised
        // Instance variable may or maynot be initilised

        public IPhone(string phoneNumber)
        {
            this.phoneNumber = phoneNumber;
            age = 3;
            speed = 8.02;
            regNumber = 1991519;
            didItRain = true;
            phoneId = 885;
        }

        public void MakeCall()
        {
            int numberOfEyes = 0;
            phoneNumber = "9819159619";
            Console.WriteLine(phoneNumber);
            numberOfEyes = 2;
            Console.WriteLine(numberOfEyes);
        }


        public static void RecieveCall(string openingMessage)
        {
            Console.WriteLine(phoneId);
            Console.WriteLine(openingMessage);
        }

        public void SellPhone()
        {
            int n = 0;
//            int result = int.Parse(phoneNumber);
            bool ans = int.TryParse(phoneNumber, out n);
            if (ans == true)
            {
                int a = n - 5;
            }else if (!ans)
            {
                Console.WriteLine("There is not a number");
            }
        }
    }
}