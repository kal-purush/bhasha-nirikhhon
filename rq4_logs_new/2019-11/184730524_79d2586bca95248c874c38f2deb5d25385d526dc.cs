using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace FindPropertiesOfClassesInAssembly
{
    class Program
    {
        static void Main(string[] args)
        {

            var assembly = Assembly.GetExecutingAssembly();
            var expoType = assembly.ExportedTypes.ToList();
            
            expoType.ForEach(et=>et.GetProperties().ToList().ForEach(etp=>Console.WriteLine($"{etp.Name} : {etp.GetValue(Activator.CreateInstance(et))}"))); //Should create Instance av Type to GetValue() otherwise got null reference exception.
            
            Console.WriteLine("Hello World!");
            Console.ReadKey();
        }
    }
}