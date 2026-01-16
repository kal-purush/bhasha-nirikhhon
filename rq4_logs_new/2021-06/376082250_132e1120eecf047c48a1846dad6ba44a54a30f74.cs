using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Globalization;




namespace Exercise._0.Prerequisite
{

    class Program
    {

        static void Main(string[] args)
        {

            DateTimeOffset dateTimeObj = DateTime.Now;

            Console.WriteLine("Exercise 0. Prerequisite");
            Console.WriteLine("------------------------------------------------------------");
            var keepAlive = true;
            while (keepAlive)
            {
                try
                {
                    Console.WriteLine("Question   number 111.   Writing to console.");
                    Console.WriteLine("Assignment number   1.   Print your first and last name in consol.");
                    Console.WriteLine("Assignment number   2.   Get date today, tomorrow and yesterday.");
                    Console.WriteLine("Question   number 222.   String manipulation.");
                    Console.WriteLine("Assignment number   3.   Ask for your first and last name and write to consol.");
                    Console.WriteLine("Assignment number   4.   Turn ..The quick fox Jumped Over the DOG..  to ..The brown fox jumped over the lazy dog..");
                    Console.WriteLine("Assignment number   5.   Turn ..Arrays are very common in programming, they look something like: [1,2,3,4,5].. ");
                    Console.WriteLine("                         to ..[1,4,5,6,7,8,9,10]..");
                    Console.WriteLine("Question   number 555.   Correct average.");
                    Console.WriteLine("Assignment number   6.   Ask for first and last number and output the biggest, smallest, difference (- ), ");
                    Console.WriteLine("                         sum(+), product(* ) and ratio( / ) between the two numbers.");
                    Console.WriteLine("Assignment number   7.   Calculate the area and volume of a circle respective sphere with the given radius.");
                    Console.WriteLine("Assignment number   8.   Input a decimal number. Then output square root of the number ");
                    Console.WriteLine("                         and the number raised to the power of 2 and 10..");



                    Console.WriteLine("---------------------------------------------------------------------------");
                    Console.Write("Enter assignment number (or 0 to exit): ");
                    var assignmentChoice = int.Parse(Console.ReadLine() ?? "");
                    Console.ForegroundColor = ConsoleColor.Green;
                    switch (assignmentChoice)
                    {
                        case 111:
                            RunExerciseq1();
                            break;
                        case 1:
                            RunExerciseOne();
                            break;
                        case 2:
                            RunExerciseTwo();
                            break;
                        case 222:
                            RunExerciseq2();
                            break;
                        case 3:
                            RunExerciseThree();
                            break;
                        case 4:
                            RunExerciseFour();
                            break;
                        case 5:
                            RunExerciseFive();
                            break;
                        case 555:
                            RunExerciseq5();
                            break;
                        case 6:
                            RunExerciseSix();
                            break;
                        case 7:
                            RunExerciseSeven();
                            break;
                        case 8:
                            RunExerciseEight();
                            break;
                        case 0:
                            keepAlive = false;
                            break;
                        default:
                            Console.ForegroundColor = ConsoleColor.Red;
                            Console.WriteLine("That is not a valid assignment number !");
                            break;
                    }
                    Console.ResetColor();
                    Console.WriteLine("Hit any key to continuel");
                    Console.ReadKey();
                    Console.Clear();
                }// try 
                catch
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("That is not a valid assignment number!");
                    Console.ResetColor();
                }

            }// while (keepAlive)


        }// static void Main(string[] args)





        private static void RunExerciseq1()
        {
            Console.WriteLine("Writing to console");
            Console.WriteLine("-----------------------");
            Console.WriteLine("Hello");  //Byter rad
            Console.WriteLine("world!");  //Byter rad
            Console.Write("Hello");   //Byter inte rad
            Console.Write(" world!");//Byter inte rad
            Console.WriteLine("  ");
            Console.WriteLine("-----------------------");
        }

        private static void RunExerciseOne()
        {
               // Console.WriteLine("Ange ditt Förnamn.");
            string firstname = "Magnus";
               //Console.WriteLine("Ange ditt efternamn.");
            string lastname = "Ivarsson";
            Console.WriteLine("Hello " + firstname + " " + lastname + "!  I’m glad to inform you that you are the test subject of my very first assignment!");
        }


        private static void RunExerciseTwo()
        {
            string[] months = {"January", "February", "March", "April", "May", "June", "July", "September", "October", "November", "December"};

            DateTime now = DateTime.Now;
            Console.WriteLine("Todays......date is {0} of {1} {2}", now.Day, months[now.Month - 1], now.Year);
            DateTime dt = DateTime.Now;
            DateTime dt1 = dt.AddDays(1);
            Console.WriteLine("Tomorrows...date is {0} of {1} {2}", dt1.Day, months[now.Month - 1], now.Year);
            DateTime dt2 = dt.AddDays(-2);
            Console.WriteLine("Yesterdays..date was {0} of {1} {2}", dt2.Day, months[now.Month - 1], now.Year);
        }


        private static void RunExerciseq2()
        {
            String part = "if you're doing your";
            part = part + " best, \t you won't have any";
            String quote = part + " time to worry about failure.";  // part add first and second part and  \t  is Tab text 
            Console.WriteLine(quote);
            Console.WriteLine("-----------------------");
        }


        private static void RunExerciseThree()
        {
            Console.Write("Enter your first name: ");
            string firstname = Console.ReadLine();
            Console.Write("Enter your last name: ");
            string lastname = Console.ReadLine();
            Console.WriteLine( firstname + " " + lastname );
        }

        private static void RunExerciseFour()
        {
            String str4 = "The quick fox Jumped Over the DOG";
                       //  The brown fox jumped over the lazy dog
            Console.WriteLine(str4);
            str4 = str4.Replace("quick", "brown");
            str4 = str4.Replace("Jumped Over", "jumped over");
            str4 = str4.Replace("DOG", "lazy dog");
            Console.WriteLine(str4);    
        }


        private static void RunExerciseFive()
        {
            String str5 = "Arrays are very common in programming, they look something like: [1,2,3,4,5]";
                                                                                        //  [1,4,5,6,7,8,9,10]
            Console.WriteLine(str5);          
            str5 = str5.Replace("Arrays are very common in programming, they look something like: [", "[");
            str5 = str5.Replace(",2,3,", ",");
            str5 = str5.Replace("]", ",6,7,8,9,10]");
            Console.WriteLine(str5);
        }



        private static void RunExerciseq5()
        {

            int a = 8;
            int b = 1;
            double average = (a + b) / (double)2;  // paranteser och  (double) i nämnaren
            Console.WriteLine(average);
            Console.WriteLine("-----------------------");
        }


        private static void RunExerciseSix()
        {
            Console.Write("Enter first number: ");
            double firstnumber6 = Convert.ToDouble(Console.ReadLine());
            Console.Write("Enter second number: ");
            double lastnumber6 = Convert.ToDouble(Console.ReadLine());
            //--------------------
            double biggest6 = 0;
            if (firstnumber6 > lastnumber6) 
            {biggest6 = firstnumber6;} else {biggest6 = lastnumber6;}
            Console.WriteLine("Biggest: " + biggest6);
            //--------------------
            double smallest6 = 0;
            if (firstnumber6 < lastnumber6)
            { smallest6 = firstnumber6; } else { smallest6 = lastnumber6; }
            Console.WriteLine("Smallest: " + smallest6);
            //-------------------
            double diff6 = biggest6 - smallest6;
            Console.WriteLine("Difference (-): " + biggest6 + " - " + smallest6 + " = " + diff6);
            //----------------------
            double sum6 = firstnumber6 + lastnumber6;
            Console.WriteLine("Sum (+): " + biggest6 + " + " + smallest6 + " = " + sum6);
            //-----------------------
            double product6 = firstnumber6 * lastnumber6;
            Console.WriteLine("Product (*): " + biggest6 + " x " + smallest6 + " = " + product6);
            //----------------------
            if (lastnumber6 == 0)
            { Console.WriteLine("Second number can not be zero. Try again."); } 
            else 
            {
                double ratio6 = firstnumber6 / (double)lastnumber6;
                Console.WriteLine("Ratio (/) : " + firstnumber6 + " / " + lastnumber6 + " = " + ratio6);
            }                      
            //----------------------


        }


        private static void RunExerciseSeven()
        {
            NumberFormatInfo nfi = new CultureInfo("en-US", false).NumberFormat;
            nfi.NumberDecimalDigits = 4;
            Console.Write("Enter radius and I will calculate area of circle and volume of sphere: ");
            double radius6 = Convert.ToDouble(Console.ReadLine());
            //--------------------
            double area6 = 3.141592 * radius6 * radius6 ;
            Console.WriteLine("Area of circle: " + area6.ToString("N", nfi));
            double volume6 = ( 4 * 3.141592 * radius6 * radius6 * radius6 ) / 3;
            Console.WriteLine("Volume of sphere: " + volume6.ToString("N", nfi));
        }


        private static void RunExerciseEight()
        {
            NumberFormatInfo nfi = new CultureInfo("en-US", false).NumberFormat;
            nfi.NumberDecimalDigits = 4;
            Console.WriteLine("Input a decimal number. ");
            double tal8 = Convert.ToDouble(Console.ReadLine());
            //------------------------
            double tal8sqrt = Math.Sqrt(tal8);
            Console.WriteLine("Square root: " + tal8sqrt.ToString("N", nfi));
            //--------------------------------------
            double tal82 = Math.Pow(tal8, 2);
            Console.WriteLine("Raised to the power of 2: " + tal82.ToString("N", nfi));
            //-----------------------------------
            double tal810 = Math.Pow(tal8, 10);
            Console.WriteLine("Raised to the power of 10: " + tal810.ToString("N", nfi));

        }


    }// class Program

}// namespace Exercise._0.Prerequisite