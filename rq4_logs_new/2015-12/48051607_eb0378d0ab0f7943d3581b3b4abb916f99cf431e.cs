using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Statistics
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("---------------Statistics Calculator---------------");

            Console.WriteLine("Do you want to enter your numbers or to read them from a file [enter/read]: ");
            var enterorread = Console.ReadLine();
           
            while (enterorread != "fet")
            {
                if (enterorread != "read" && enterorread != "enter")
                {
                    Console.WriteLine("Please write 'enter' if you want to enter your numbers or 'read' if you want to read them from a file: ");
                    enterorread = Console.ReadLine();
                }
                if (enterorread == "enter")
                {
                    List<int> numberList = CollectData();
                    Calculate(numberList);
                    enterorread = "fet";
                    Console.ReadKey();
                }
                if (enterorread == "read")
                {
                    string readlocation = @"D:\Gerard\GitHub\code-challenge-2\Statistics\File.txt";
                    Console.WriteLine("Please note that the numbers will be read from ["+readlocation+"] file: ");
                    Console.WriteLine("The file must contain only one int for line");
                    

                    string[] listnumstring = System.IO.File.ReadAllLines(readlocation);
                    var numberList = listnumstring.Select(x => Int32.Parse(x)).ToList();
                    Calculate(numberList);
                    enterorread="fet";
                    Console.ReadKey();
                }
                
            }
                Console.ReadKey();
            }

        private static List<int> CollectData()
        {
            Console.WriteLine("Please enter a number: ");
            var number = Console.ReadLine();
            List<int> numberList = new List<int>();

            while (number != "done")
            {
                while (!ValidInput.Validate(ValidInput.Number, number))
                {
                    Console.WriteLine("This is not a float number. Please enter the number again: ");
                    number = Console.ReadLine();
                    if (number == "done") {
                        return numberList;
                    }
                }

                int numberint = Convert.ToInt32(number);
                numberList.Add(numberint);

                Console.WriteLine("Please enter a number: ");
                number = Console.ReadLine();
            }
            string NumbersString = string.Join(",", numberList.ToArray());
            Console.WriteLine(" Your numbers are: " + NumbersString);
            return numberList;
        }

        private static void Calculate(List<int> numberList)
        {

            Console.WriteLine("");//Blank spaces only for stetics
            Console.WriteLine("");
            //Average
            double average = numberList.Average();
            Console.WriteLine(" The average is: " + average.ToString("0.000"));
            //Min
            double min = numberList.Min();
            Console.WriteLine(" The min is: " + min.ToString("0.000"));
            //Max
            double max = numberList.Max();
            Console.WriteLine(" The max is: " + max.ToString("0.000"));
            //Standard deviation
            double dif = 0;
            List<double> numberList2 = new List<double>();
            foreach (int value in numberList)
            {
                double doublevalue = System.Convert.ToDouble(value);
                dif = Math.Pow((doublevalue - average), 2);
                numberList2.Add(dif);

            }
            double average2 = numberList2.Average();
            double standarddeviation = Math.Sqrt(average2);
            Console.WriteLine(" The standard deviation is: " + standarddeviation.ToString("0.000"));

            DrawDistribution(numberList, max);   //draw the distribuiton of the numbers

        }

        private static void DrawDistribution(List<int> numberList, double max)
        {
            double p20 = max / 100 * 20;  //Draw a diagram of the distribution
            double p40 = max / 100 * 40;
            double p60 = max / 100 * 60;
            double p80 = max / 100 * 80;
            string s20 = "00/20%  : ";
            string s40 = "20/40%  : ";
            string s60 = "40/60%  : ";
            string s80 = "60/80%  : ";
            string s100 = "80/100% : ";

            foreach (var newnumber in numberList)
            {
                if (newnumber <= p20)
                {
                    s20 = s20 + "|";
                }
                if (newnumber > p20 && newnumber <= p40)
                {
                    s40 = s40 + "|";
                }
                if (newnumber > p40 && newnumber <= p60)
                {
                    s60 = s60 + "|";
                }
                if (newnumber > p60 && newnumber <= p80)
                {
                    s80 = s80 + "|";
                }
                if (newnumber > p80)
                {
                    s100 = s100 + "|";
                }

            }


            Console.WriteLine(""); //Blank spaces only for stetics
            Console.WriteLine(""); //Blank spaces only for stetics
            Console.WriteLine("The distribution of the numbers is (% of the max):");
            Console.WriteLine("");
            Console.WriteLine(s20);
            Console.WriteLine(s40);
            Console.WriteLine(s60);
            Console.WriteLine(s80);
            Console.WriteLine(s100);
        }

        }
    }