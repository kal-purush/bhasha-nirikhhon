using System;

namespace program
{

    class bike
    {
        public string model = "Continental GT"; 
    }

    class PrivateModifier
    {
        
        static void Main(string[] args)
        {
              
            bike obj1 = new bike();
            Console.WriteLine(obj1.model); 

           
        
        }
    }
}