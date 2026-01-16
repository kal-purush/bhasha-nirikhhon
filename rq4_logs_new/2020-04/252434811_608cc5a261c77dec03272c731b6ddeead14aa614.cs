using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HomeWork11
{
    class DZ11
    {
        public static void Start()
        {

            Student Stud1 = new Student()
            {
                Name = "Oleg",
                Surname = "Smirnov",
                Department = "RadioEnginearing"
            };

            Student Stud2 = new Student()
            {
                Name = "Nikolaj",
                Surname = "Semenkov",
                Department = "RadioEnginearing"
            };

            Stud1.Work();
            Stud2.Work();

            Console.WriteLine($"This students ({Stud1} & {Stud2}) are{(Stud1.Equals(Stud2) ? "" : " not")} the same\n");

            //***********

            Teacher Teacher1 = new Teacher()
            {
                Name = "Vladislav",
                Surname = "Pimenov",
                Department = "Menagment",
                Degree = "PhD"
            };

            Teacher Teacher2 = new Teacher()
            {
                Name = "Vladislav",
                Surname = "Pimenov",
                Department = "RadioEnginearing",
                Degree = "Professor"
            };

            Teacher1.Work();
            Teacher2.Work();

            Console.WriteLine($"This teachers ({Teacher1} & {Teacher2}) are{(Teacher1.Equals(Teacher2) ? "" : " not")} the same\n");

            Console.ReadLine();

        }

        public static bool IsTwoObjectIsEqualsByName<T1, T2>(T1 firstObject, T2 secondObject)
        {
            bool isEquals = false;

            if (firstObject.GetType() == secondObject.GetType())
            {
                if (firstObject.ToString() == secondObject.ToString())
                {
                    isEquals = true;
                }
            }

            return isEquals;
        }
    }

    abstract class Person
    {
        public string Name { get; set; }
        public string Surname { get; set; }
        public string Department { get; set; }

        public override string ToString()
        {
            return Name +" "+ Surname;
        }

        public abstract void Work();
    }

    class Student : Person
    {
        public override void Work()
        {
            Console.WriteLine($"I'm student. My name is {this}. I'm going to study on {Department} department");
        }

        public override bool Equals(Object obj)
        {
            return DZ11.IsTwoObjectIsEqualsByName<Student, Student>(this, (Student)obj) 
                && this.Department == ((Student)obj).Department;
        }

        
    }

    class Teacher : Person
    {
        public string Degree;

        public override void Work()
        {
            Console.WriteLine($"I'm teacher. My name is {this}. I've {Degree} degree. " +
                $"I'm going to train students on {Department} department");
        }

        public override bool Equals(object obj)
        {
            return DZ11.IsTwoObjectIsEqualsByName<Teacher, Teacher>(this, (Teacher)obj)
                && this.Department == ((Teacher)obj).Department
                && this.Degree == ((Teacher)obj).Degree;
        }
    }

    class HeadOfDepartment : Teacher
    {
        public override void Work()
        {
            Console.WriteLine($"I'm Head of Depertment. My name is {this}.  I've {Degree} degree. " +
                $"I'm going to organize teachers on {Department} department");
        }

        public override bool Equals(object obj)
        {
            return DZ11.IsTwoObjectIsEqualsByName<HeadOfDepartment, HeadOfDepartment>(this, (HeadOfDepartment)obj)
                && this.Department == ((HeadOfDepartment)obj).Department
                && this.Degree == ((HeadOfDepartment)obj).Degree;
        }
    }
}