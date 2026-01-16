using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.Entity;
namespace Factory
{
    static class Controller
    {
        private static dbFChocolate db = new dbFChocolate();
        private static string name, lastname, email;
        private static IQueryable<PersonProfile> nameIQ;
        private static int age;

        public static void CreatedUser()
        {
  
            do
            {
                Console.WriteLine("Name: ");
                name = Console.ReadLine();

                Console.WriteLine("Last name: ");
                lastname = Console.ReadLine();

                Console.WriteLine("Age: ");
                age = int.Parse(Console.ReadLine());

                Console.WriteLine("Email: ");
                email = Console.ReadLine();
            } while (name == "" || lastname == "");


            PersonProfile person = new PersonProfile(name,lastname,age,email);
            db.User_profile.Add(person);
            db.SaveChanges();
        }

        private static IQueryable<PersonProfile> Find_PersonByName()
        {
            Console.WriteLine("Buscar Perfil: ");
            name = Console.ReadLine();
            nameIQ = from person in db.User_profile where person.FirstName.Contains(name) select person;
            if (!nameIQ.Any())
            {
                Console.WriteLine("No se encuentra Este perfil No existe");
                ShowUserInfo();
            }
            Console.WriteLine("Este perfil existe");
            return nameIQ;
        }

        public static void ShowUserInfo()
        {
            Find_PersonByName();
            foreach (var p in nameIQ)
            {
                Console.WriteLine(p.FirstName + " " + p.LastName + ", Cantidad pagada: " + p.money.mount + " Falta por pagar " + p.money.oweMoney);
                break;
            }

        }

        public static void UserPayMoney()
        {
            Console.WriteLine("Cuanto pago: ");
            int pay = int.Parse(Console.ReadLine());
            foreach (var p in nameIQ)
            {
                if (p.money.oweMoney >=0)
                {
                    p.money.oweMoney = p.money.oweMoney - pay;
                    p.money.mount = p.money.mount + pay;
                   
                    if (p.money.oweMoney < 0)
                        p.money.oweMoney = 0;
                }
                db.Entry(p).State = EntityState.Modified; 
            }
            
            db.SaveChanges(); 
            
        }

        public static void UserOweMoney() 
        {
            Console.WriteLine("Cuanto debe: ");
            int owe = int.Parse(Console.ReadLine());
            foreach (var p in nameIQ)
            {
                p.money.oweMoney = p.money.oweMoney + owe;
                db.Entry(p).State = EntityState.Modified;
            }
            db.SaveChanges();
        }



        //private static void SelectMoneyByid(int id)
        //{
        //    var mount = from money in db.PayChocolates where money.IdMoney == id  select money;
        //    foreach (var i in mount)
        //    {
        //        Console.Write(i.mount);
        //
    
    }
            
}

