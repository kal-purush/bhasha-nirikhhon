internal class Program
{
    private static void Main(string[] args)
    {
        {
            int x;
            double y;
            Console.WriteLine("math:\n");
            x = Convert.ToInt32(Console.ReadLine());
            y = Math.Sqrt(x) + 10;
            Console.WriteLine($"y = {y}");

        }
    }
}
