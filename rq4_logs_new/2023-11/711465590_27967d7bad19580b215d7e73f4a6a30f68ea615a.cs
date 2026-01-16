using System;
using System.Diagnostics;

namespace MembersFinalizersDestructors
{
    public class Members
    {
        // Member - private field
        private string memberName;
        private string jobTitle;
        private int salary = 20000;
        // Member - public field
        public int age;

        // Member constructor
        public Members()
        {
            age = 30;
            memberName = "Lucy";
            salary = 60000;
            jobTitle = "Developer";

            Console.WriteLine("Object created");
        }

        // Member - finalizer - destructor
        ~Members()
        {
            // cleanup statement
            Console.WriteLine("Deconstruction of Member object");
            Debug.WriteLine("Deconstruction of Member object");
        }

        // Member - property - exposes jobTitle safely
        public string JobTitle
        {
            get { return jobTitle; }
            set { jobTitle = value; }
        }

        // public member methods
        public void Introducing(bool isFriend)
        {
            if (isFriend)
            {
                SharingPrivateStuff();
            }
            else
            {
                Console.WriteLine("Hi my name is {0}, my job title is {1}, i'm {2} year old", memberName, jobTitle, age);
            }
        }

        // private member methods
        private void SharingPrivateStuff()
        {
            Console.WriteLine("My salary is {0}", salary);
        }
    }
}