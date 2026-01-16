using System;
using NinjaDomain.Classes;
using NinjaDomain.DataModel;
using System.Data.Entity;
using System.Collections.Generic;

namespace ConsoleApplication
{
    public class Program
    {
        static void Main(string[] args)
        {
            Database.SetInitializer(new NullDatabaseInitializer<NinjaContext>());
            InsertNinja();
            //InsertMultipleNinjas();
            //InsertNinjaWithEquipment();
            //InsterNinjaQuery();
            //InstertNinjaGraphQuery();
            //QueryAndUpdateNinja();
            //QueryAndUpdateNinjaDisconnected();
            //DeleteNinja();
            Console.ReadKey();
        }

        private static void InsertNinja()
        {
            var ninja1 = new Ninja
            {
                Name = "SampsonSan",
                ServedInOniwaban = false,
                DateOfBirth = new DateTime(2008, 1, 28),
                ClanId = 2,
            };
            var ninja2 = new Ninja
            {
                Name = "simpsoenon",
                ServedInOniwaban = true,
                DateOfBirth = new DateTime(2009, 1, 29),
                ClanId = 3,
            };
            using (var context = new NinjaContext())
            {
                context.Database.Log = Console.Write;
                context.Ninjas.AddRange(new List<Ninja> { ninja1, ninja2 });
                context.SaveChanges();
            }
        }
    }
}