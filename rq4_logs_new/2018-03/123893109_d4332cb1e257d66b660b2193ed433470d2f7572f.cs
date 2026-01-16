using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DABHandin2._1
{
    class Person
    {
        public Context PersonContext { get; set; }
        public Mail PersonMail { get; set; }
        public List<HelpClass> HelpClass { get; set; }
        public List<Phone> PersonPhone { get; set; }
        public Adress RegisteredAdress { get; set; }

        public Person(string personContext,string mailAdress,Adress registerAdress,List<HelpClass> helpClass,List<Phone> phoneNums)
        {
            PersonContext=new Context(personContext);
            PersonMail = new Mail(mailAdress);
            HelpClass = helpClass;
            PersonPhone = phoneNums;
            RegisteredAdress = registerAdress;
        }

        public void PresentPerson()
        {
            Console.WriteLine("The persons Context is: " + PersonContext.PersonContext);
            Console.WriteLine("Her/his Registered adress is: "+ RegisteredAdress.AdressName);
            Console.WriteLine("Her/his Mailadress is : " +PersonMail.MailAdress);
            Console.WriteLine("");
            Console.WriteLine("He/She is connected to following adresses:");
            foreach (var adress in HelpClass)
            {
                Console.WriteLine("AdressType: " + adress.Type + " is at: " + adress.Adress.AdressName);
            }

            Console.WriteLine("");
            Console.WriteLine("He/She has the following phoneNumbers:");
            foreach (var numbers in PersonPhone)
            {
                Console.WriteLine("PhoneNumber: " + numbers.PhoneNumber + " And the privider is named: "+ numbers.Provider.CompanyName);
            }
            Console.WriteLine("_______________________________________________________________");
            Console.WriteLine("");
            
        }

    }
}