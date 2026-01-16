using System;

namespace LearningCSharp.Lessons
{
    public class Strings
    {
        [Demo]
        public static void NullString1()
        {
            object obj1 = null;
            object obj2 = "Hi";
            Console.WriteLine((string)obj1 + (string)obj2);
            Console.WriteLine((string)obj2 + (string)obj1);
        }

        [Demo]
        public static void NullString2()
        {
            Console.WriteLine(((string)null).ToString());
        }

        [Demo]
        public static void NullString3()
        {
            Console.WriteLine((string)null);
        }

        [Demo]
        public static void StringEquals()
        {
            var a = "a";
            var aa = "aa";
            var aa2 = a + a;

            Console.WriteLine(aa == aa2);
            Console.WriteLine((object)aa == (object)aa2);
            Console.WriteLine(string.ReferenceEquals(aa, aa2));
        }
    }
}