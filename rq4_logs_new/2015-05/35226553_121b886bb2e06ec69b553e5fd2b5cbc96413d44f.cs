using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AsyncAwait
{
    class Program
    {
        static void Main(string[] args)
        {
            Program p = new Program();
            p.ReturnTaskWithResultMethodAsync();
        }

        public Task AsyncMethod()
        { Console.WriteLine("I am AsyncMethod"); Task t = new Task(() => { Console.WriteLine("Hello World"); }); t.Start(); return t; }
        public Task<string> AsyncMethodWithStringReturn()
        {
            Console.WriteLine("I am AsyncMethodWithStringReturn");
            Task<string> t = new Task<string>(() => { return "Text"; });
            t.Start(); t.Wait(); return t;
        }

        public async void ReturnVoidMethodAsync()
        {
            await AsyncMethod();
            Console.WriteLine("I am ReturnVoidMethodAsync");
        }

        public async Task ReturnTaskMethodAsync()
        {
            await AsyncMethod();
            Console.WriteLine("I am ReturnTaskMethodAsync");
        }

        public async Task<string> ReturnTaskWithResultMethodAsync()
        {
            string text = await AsyncMethodWithStringReturn();
            Console.WriteLine("ReturnTaskWithResultMethodAsync");
            Console.WriteLine("And the text is: " + text);
            return text;
        }
    }
}