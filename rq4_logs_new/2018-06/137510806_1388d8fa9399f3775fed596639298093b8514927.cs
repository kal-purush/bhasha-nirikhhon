using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Reflection
{
    class Program
    {
        static void Main(string[] args)
        {
            //zadanie1();
            zadanie2();
        }
            public static void zadanie1()
        {
            Type console = typeof(Console);
            MethodInfo[] methods = console.GetMethods();
            Console.WriteLine("*** Список методов класса Console ***\n");
            foreach (MethodInfo m in methods)
            {
                Console.Write("-->" + m.ReturnType.Name + "\t" + m.Name + "(");
                ParameterInfo[] p = m.GetParameters();
                for (int i = 0; i < p.Length; i++)
                {
                    Console.Write(p[i].ParameterType.Name + " " + p[i].Name);
                    if (i + 1 < p.Length) Console.Write(", ");
                }
                Console.Write(")\n");
            }

            Console.ReadLine();
        }

        public static void zadanie2()
        {
            User user = new User()
            {
                Id = 1,
                Login = "admin",
                Password = "verystrongpassword"
            };

            Type type = typeof(User);

            Console.WriteLine("Type is: {0}", type.Name);
            PropertyInfo[] props = type.GetProperties();

            Console.WriteLine("Properties (N = {0}):",
                              props.Length);

            foreach (var prop in props)
                if (prop.GetIndexParameters().Length == 0)
                    Console.WriteLine("   {0} ({1}): {2}", prop.Name,
                                      prop.PropertyType.Name,
                                      prop.GetValue(user, null));

            Console.ReadLine();
        }

    
        }
    }
