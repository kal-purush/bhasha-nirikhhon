using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RJL.HW4.OOP.Classes.Task3
{
    class Program
    {
        static void Main(string[] args)
        {

            Shop mobileShop = new Shop(GetInputNameShop());
            Console.WriteLine("=>Great, the name of shop is "+ mobileShop.Name);
            //Storage mobileStorage1 = new Storage(1,"Okruzhnaya,10");
            //Storage mobileStorage2 = new Storage(2, "Klavdiivska,40");
            //Storage mobileStorage3 = new Storage(3, "Uborevicha,20");
            //mobileShop.AddStorage(mobileStorage1);
            //Phone phone1 = new Phone("Samsung Galaxy J6", 600);
            //Phone phone2 = new Phone("IPhone S8", 1500);
            //Phone phone3 = new Phone("Sony Xperia XZ", 800);
            //mobileStorage1.AddPhonetoStorage(phone1);
            Console.ReadLine();
        }
        static string GetInputNameShop()
        {
            string inputName;
            do
            {
                Console.WriteLine("Please write valid (not empty) name for shop");
                inputName = Console.ReadLine();
            }
            while (inputName == ""|| inputName == " "|| inputName==null);
            
            return inputName;
           
        }
    }
}