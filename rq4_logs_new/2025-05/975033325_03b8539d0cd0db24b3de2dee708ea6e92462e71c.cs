namespace Practice2.Task1f
{
    internal class Program
    {
        static void Main(string[] args)
        {
            string wordString = "";
            int n = 0;
            do
            {
                Console.WriteLine("Введите слово:");
                string word = Console.ReadLine();
                wordString = wordString + " " + word;
                n++;
            }
            while (n < 3);

            Console.WriteLine(wordString);

        }
                 
        
    }
}