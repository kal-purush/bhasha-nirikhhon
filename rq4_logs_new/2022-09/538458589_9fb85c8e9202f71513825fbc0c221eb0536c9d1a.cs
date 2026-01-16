using System;

namespace TMPS
{
    public class Employee
    {
        private string id;
        private string name;
        private int age;
        private double salary;
        private int premium;
        private DateTime vacayStart;
        private DateTime vacayEnd;

        public string ID
        {
            get { return id; }
            set { id = value; }
        }

        public string Name
        {
            get { return name; }
            set { name = value; }
        }

        public int Age
        {
            get { return age; }
            set { age = value; }
        }

        public double Salary
        {
            get { return salary; }
            set { salary = value; }
        }

        public int Premium
        {
            get { return premium; }
            set { premium = value; }
        }

        public DateTime VacationStart
        {
            get { return vacayStart; }
            set { vacayStart = value; }
        }
        public DateTime VacationEnd
        {
            get { return vacayEnd; }
            set { vacayEnd = value; }
        }


        public Employee(string id, string name, int age,
                        double salary, int premium, DateTime vacayStart, DateTime vacayEnd)
        {
            ID = id;
            Name = name;
            Age = age;
            Salary = salary;
            Premium = premium;
            VacationStart = vacayStart;
            VacationEnd = vacayEnd;
        }

    }
}