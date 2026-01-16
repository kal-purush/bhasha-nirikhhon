using System.Net;

class Program
{
    // TargetHeartRate = (((220 − age) − restingHR) × intensity) + restingHR
    static void Main()
    {
        Console.WriteLine("Karvonen Heart Rate Calculator");
        Console.WriteLine("----------------------");
        int age = GetUserInput("Enter your age: ");
        int restingHR = GetUserInput("Enter your resting heart rate: ");
        
        Console.WriteLine("\nIntensity | Rate");
        Console.WriteLine("----------|--------");

        for (int intensity = 55; intensity <= 95; intensity += 5)
        { 
            double targetHeartRate = (((220.0 - age) - restingHR) * intensity / 100.0) + restingHR;
            Console.WriteLine($"{intensity}%       | {Math.Round(targetHeartRate)} bpm");
        }
    }
    static int GetUserInput(string prompt)
    {
        int input;
        
        do
        {
            Console.Write(prompt);
            
            if (!int.TryParse(Console.ReadLine(), out input))
            {
                Console.WriteLine("Invalid input, please enter a number. ");
            }
            else
            {
                Console.WriteLine($"You have entered {input}.");
                break;
            }
        } while (true);
        return input;
    }
}