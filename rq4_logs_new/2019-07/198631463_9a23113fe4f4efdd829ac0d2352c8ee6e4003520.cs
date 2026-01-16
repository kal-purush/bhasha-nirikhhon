using System;

namespace GetToKnowYourClassmates
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello, which student are you inquiring about? Please enter a number 1-5 to learn more about each student. ");
            bool go = true;
            while(go)
            {

                string[] students = { "1. Aaron Aaronson", "2. Beth Bethonson", "3. Chad Chadwick", "4. Donna Donaldson", "5. Eric Ericson" };
                string[] homeTown = { "Detroit, Mi", "Memphis, Mi", "Monroe, Mi", "St Joe, Mi", "Traverse City, Mi" };
                string[] favFood = { "Pizza", "Tacos", "Burgers", "Coney", "Thai" };
                string[] animal = { "Dog", "Cat", "Snake", "Hedgehog", "Elephant" };

                DisplayStudents(students);

                try
                {
                    int input = UserInput();
                    DisplayInfo(input - 1, students);
                }
                catch
                {
                    Console.WriteLine("Please enter a valid number, 1-5.");
                }

                Console.WriteLine("Would you like to know about this students favorite: ");
                Console.WriteLine("1. Food");
                Console.WriteLine("2. Hometown");
                Console.WriteLine("3. Animal");

                int input2 = UserInput2();
                try
                {
                    //int input2 = UserInput2();
                    DisplayFood(input2 - 1, favFood);
                    DisplayTown(input2 - 1, homeTown);
                    DisplayAnimal(input2 - 1, animal);
                }
                catch
                {
                    Console.WriteLine("Please enter a valid number, 1-3.");
                }
            }
        }

        public static void DisplayStudents(string[] students)
        {
            for (int i = 0; i < students.Length; i++)
            {
                Console.WriteLine($"{students[i]}");
            }
        }

        public static int UserInput()
        {
            return int.Parse(Console.ReadLine());
        }

        public static int UserInput2()
        {
            return int.Parse(Console.ReadLine());
        }

        public static void DisplayInfo(int input, string[] students)
        {
            Console.WriteLine("The student you picked: ");
            Console.WriteLine($"{students[input]}");
        }
        public static void DisplayFood(int input2, string[] favFood)
        {
            Console.WriteLine("The student you picked loves: ");
            Console.WriteLine($"{favFood[input2]}");
        }
        public static void DisplayTown(int input2, string[] homeTown)
        {
            Console.WriteLine("The student you picked is from: ");
            Console.WriteLine($"{homeTown[input2]}");
        }
        public static void DisplayAnimal(int input2, string[] animal)
        {
            Console.WriteLine("The student you picked loves the animal: ");
            Console.WriteLine($"{animal[input2]}");
        }
    }
}