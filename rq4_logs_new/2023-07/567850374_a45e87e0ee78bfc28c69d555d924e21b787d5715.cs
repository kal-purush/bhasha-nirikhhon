using System;

namespace Dictionary
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Dictionary<string, string> cookies = new Dictionary<string, string>();

            cookies["key"] = "1";
            cookies["user"] = "testUser";
            cookies["email"] = "teste@gmail.com";

            //try
            //{
            //    Console.WriteLine(cookies["Chave"]);
            //}
            //catch (Exception ex)
            //{
            //    Console.WriteLine("Deu ruim");
            //}

            foreach(var cookie in cookies)
            {
                Console.WriteLine(cookie);
            }
        }
    }
}