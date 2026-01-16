using System;

namespace Exercise9
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Enter test data:\n" +
                              "Enter Distance in meters :");
            int distanceInMeters = Convert.ToInt32(Console.ReadLine());
            Console.WriteLine("Enter Hours:");
            int inputHour = Convert.ToInt32(Console.ReadLine());
            Console.WriteLine("Enter Minutes:");
            int inputMInutes = Convert.ToInt32(Console.ReadLine());
            Console.WriteLine("Enter Seconds:");
            int inputSeconds = Convert.ToInt32(Console.ReadLine());

            int countOfSeconds = inputHour * 3600 + inputMInutes * 60 + inputSeconds ;

            double metersPerSec = distanceInMeters / Convert.ToDouble(countOfSeconds);
            double kmPerHours = metersPerSec * 3.6;
            double milesPerHour = kmPerHours / 1.609;

            Console.WriteLine("Test Data \n" +
                              $"Input distance in meters: {distanceInMeters}\n" +
                              $"Input hour: {inputHour}\n" +
                              $"Input minutes: {inputMInutes}\n" +
                              $"Input seconds: {inputSeconds}\n" +
                              "Expected Output :\n" +
                              $"Your speed in meters/second is {metersPerSec}\n" +
                              $"Your speed in km/h is {kmPerHours}\n" +
                              $"Your speed in miles/h is {milesPerHour}");
        }
    }
}