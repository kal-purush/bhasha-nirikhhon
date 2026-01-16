namespace TernaryChallenge;

class Program
{
    static void Main(string[] args)
    {
        Console.WriteLine("\n - - Challenge - - -\n");

        int inputTemperature = 0;
        string temperatureMessage = string.Empty;
        string inputValue = string.Empty;

        Console.WriteLine("what's the current temperature ?");
        inputValue = Console.ReadLine();

        bool validInteger = int.TryParse(inputValue, out inputTemperature);

        if (validInteger)
        {
            // Nested Ternary - not sure it is a good practice:
            temperatureMessage = inputTemperature <= 15 ?
                // true
                "it is is too cold here" :
                // false
                (inputTemperature >= 16 && inputTemperature <= 28) ?
                // true
                "it is cold here" :
                // false
                inputTemperature > 28 ?
                //true
                "it is hot here" :
                // false
                "";

            Console.WriteLine(temperatureMessage);
        }
        else
        {
            Console.WriteLine("it is not a number !");
        }
    }
}
