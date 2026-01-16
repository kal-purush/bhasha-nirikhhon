using System;

namespace FollowingAlongWithUdemyCourse
{
    class Program
    {
        static void Main(string[] args)
        {
            Person personOne = new Person("Mario", "Hoyos", DateTime.Now, "Colombia" );
            Console.WriteLine(personOne.FirstName);
        }

    }

    class Person 
    {
        string firstName;
        string lastName;
        DateTime birthDate;
        string country;

        public Person(string fName,string lName,DateTime bDay,string country)
        {
            this.firstName = fName;
            this.lastName = lName;
            this.birthDate = bDay;
            this.country = country;

        }




        public string FirstName
        {
            get { return firstName;  }
            set { firstName = value; }
        }

        public string LastName 
        {
            get { return lastName; }
            set { lastName = value; }
        }

        public DateTime BirthDate
        {
            get { return birthDate; }
            set { birthDate = value; }
        }

        public string Country
        {
            get { return Country; }
            set { country = value; }
        }
    }
}