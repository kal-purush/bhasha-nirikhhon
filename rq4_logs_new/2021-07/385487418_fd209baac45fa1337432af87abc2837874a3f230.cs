using System;

namespace Excercise9
{
    class Program
    {
        static void Main(string[] args)
        {
            const double height = 1.86; //meters
            const double weight = 85; //kilograms
            double BMI = weight * 2.20462262 * 703 / height / height / 1550; //2.20462262 and 1550 - recalculation to metric system
            if (BMI > 25) Console.WriteLine($"You are overweight. BMI is {BMI.ToString("0.00")}");
            else if (BMI < 18.5) Console.WriteLine($"You are underweight. BMI is {BMI.ToString("0.00")}");
            else Console.WriteLine($"You are of optimal weight. BMI is {BMI.ToString("0.00")}");

            Console.ReadKey();  
        }
    }
}