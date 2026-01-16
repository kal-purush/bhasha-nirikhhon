using System;
using System.Threading.Tasks;
using static SingletonDesignPattern.Singleton;

namespace SingletonDesignPattern
{
    class Program
    {
        static void Main(string[] args)
        {
            
            Singleton objs = Singleton.GetInstance;
            objs.printdeatails("Singleton 1st msg");

            Singleton objEmp = Singleton.GetInstance;
            objEmp.printdeatails("Emp 1st msg");

            //Thread-safe Singleton Design Pattern
            //Parallel.Invoke(
            //   () => PrintTeacherDetails(),
            //   () => PrintStudentdetails()
            //   );
            

            //obj.printdeatails
            Console.ReadLine();
        }

        private static void PrintStudentdetails()
        {
            Singleton objs = Singleton.GetInstance;
            objs.printdeatails("Singleton Student details");
        }

        private static void PrintTeacherDetails()
        {
            Singleton objs = Singleton.GetInstance;
            objs.printdeatails("Singleton Teacher details");
        }
    }
}
/*
 * Singleton is creational pattern : object creating machenisiem
     * This pattern ensure that only one object of perticular class
     * need to be created.All further refrences to the objects are refrred to the same 
     * underlying instance created.That instance will responsible to cordinetes to 
     * in whole applications.
     * 
     * We created the Singleton class as sealed which ensures that the class cannot be
     * inherited and object instantiation is restricted in the derived class.
     * The class is created with a private constructor which will ensure that the class is not going to be instantiated from outside the class.
     * Again we declared the instance variable as private and also initialized it with the null value which ensures
     * that only one instance of the class is created based on the null condition. The public property GetInstance is
     * used to return only one instance of the class by checking the value of the private variable instance. 
     * The public method PrintDetails can be invoked from outside the class through the singleton instance.
    
 */