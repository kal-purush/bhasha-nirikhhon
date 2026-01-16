using System;

namespace Lesson_1.Homework_1
{
    class Program
    {
        public class TestCase
        {
            public int Number { get; set; }
            public string Expected { get; set; }
            public Exception ExpectedException { get; set; }
        }

        static void Test(TestCase testCase)
        {
            try
            {
                string checkResult = CheckSimple(testCase.Number);
                if (checkResult == testCase.Expected)
                {
                    Console.WriteLine("VALID TEST");
                }
                else
                {
                    Console.WriteLine("NOT VALID TEST");
                }
            }
            catch (Exception ex)
            {
                if (testCase.ExpectedException != null)
                {
                    Console.WriteLine("VALID TEST");
                }
                else
                {
                    Console.WriteLine("NOT VALID TEST");
                }

            }

        }

        static void Main(string[] args)
        {
            TestCase testCase1 = new TestCase()
            {
                Number = 11,
                Expected = "Простое",
                ExpectedException = null
            };

            TestCase testCase2 = new TestCase()
            {
                Number = 24,
                Expected = "Не простое",
                ExpectedException = null
            };

            TestCase testCase3 = new TestCase()
            {
                Number = 6781,
                Expected = "Простое",
                ExpectedException = null
            };


            TestCase testCase4 = new TestCase()
            {
                Number = -5,
                Expected = "",
                ExpectedException = new Exception()
            };

            TestCase testCase5 = new TestCase()
            {
                Number = 9,
                Expected = "Простое",
                ExpectedException = null
            };

            Test(testCase1);
            Test(testCase2);
            Test(testCase3);
            Test(testCase4);
            Test(testCase5);
            Console.ReadKey();
        }

        static string CheckSimple(int number)
        {
            int d = 0;
            int i = 2;
            while (i < number)
            {
                if (number % i == 0)
                    d++;
                i++;
            }

            if (d == 0)
                return "Простое";
            else
                return "Не простое";
        }
    }
}