using Services;

namespace MetodosGenericos
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            MainService<int> mainInt = new MainService<int>();
            MainService<string> mainString = new MainService<string>();

            mainInt.AddInfo(1);
            mainInt.AddInfo(2);

            mainString.AddInfo("Relou");
            mainString.AddInfo("Roud");

            Console.WriteLine("Return Int");
            mainInt.ReturnCollection();

            Console.WriteLine("Return String");
            mainString.ReturnCollection();
        }
    }
}