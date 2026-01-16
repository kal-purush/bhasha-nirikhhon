using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WorldGame
{

    public class View //for GUI
    {
        public  View() 
        {
            
        }

    }

    public class Model //for functions
    {

        public Model()
        {
            
        }

    }

    public class program //main app
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello! You must build industries and do science to get further out in space and find new planets.");
            Console.WriteLine("Do you wish to continue?");

            if(Console.ReadLine() == "yes")
            {
                Console.WriteLine("Good. ")
            }

        }
    }
}