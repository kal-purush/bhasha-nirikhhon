
using System;

namespace Base_Lesson_9.Lesson
{
    public static partial class Lesson
    {
        public static void DeconstructExample()
        {
            Person person1 = new Person() { FirstName = "Olga", LastName = "Rondareva" };
            Person2 person2 = new Person2() { FirstName = "Lena", LastName = "Putina" };

            var name = person1.FirstName;
            var surname = person1.LastName;

            var (name1, surname1, fullname) = person1;
            var (name2, surname2) = person2;

            Console.WriteLine(fullname);
        }
    }

    public class Person
    {
        public string FirstName { get; set; }
        public string LastName { get; set; }   

        public void Deconstruct(out string fn, out string ln, out string fl)
        {
            fn = FirstName;
            ln = LastName;
            fl = $"{FirstName} {LastName}";
        }
    }

    public class Person2
    {
        public string FirstName { get; set; }
        public string LastName { get; set; }
    }

    public static class PersonExtention
    {
        public static void Deconstruct(this Person2 p, out string fn, out string ln)
        {
            fn = p.FirstName;
            ln = p.LastName;
        }
    }
}