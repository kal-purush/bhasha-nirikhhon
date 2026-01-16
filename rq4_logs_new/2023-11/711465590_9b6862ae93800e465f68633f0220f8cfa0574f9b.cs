namespace NestedIfs;

class Program
{
    static void Main(string[] args)
    {
        bool isAdmin = false;
        bool isRegistered = true;

        Console.WriteLine("Pleas enter your username : ");
        string userName = "";
        userName = Console.ReadLine();

        if (isRegistered && userName != "" && userName.Equals("admin"))
        {
            Console.WriteLine("Hello there, registerd user");
            //if (userName != "")
            //{
            Console.WriteLine("Hello there, " + userName);
            //if (userName.Equals("admin"))
            //{
            Console.WriteLine("Hello there, Admin");

            //        }
            //    }

        }
        if (isRegistered || isAdmin)
        {
            Console.WriteLine("Hello there, You are registered");
        }
        Console.Read();
    }
}