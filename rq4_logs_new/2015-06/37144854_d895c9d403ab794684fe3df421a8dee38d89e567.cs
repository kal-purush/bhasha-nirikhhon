using DesignPatterns.NullObject.Animals;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DesignPatterns.NullObject
{
    public class Program
    {
        private static CatManagement _catManagement = new CatManagement();
        
        static void Main(string[] args)
        {
            IAnimal cat = _catManagement.FormatterCatToRun(null);            
            cat.Run();

            cat = new Cat();
            cat = _catManagement.FormatterCatToRun(cat);
            cat.Run();

            Console.ReadKey();
        }
    }
}