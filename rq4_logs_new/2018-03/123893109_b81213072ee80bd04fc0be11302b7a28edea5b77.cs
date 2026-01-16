using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using DAB2_2RDB;

namespace DAB2_2
{
    class Program
    {
        static void Main(string[] args)
        {
            var context = new Dab2_2RdbContext();
            var personRepo = new Repository<Person>(context);
            var phoneNumberRepo = new Repository<PhoneNumber>(context);

            var person = new Person()
            {
                FirstName = "Kasper",
                LastName = "Hermansen",
                Email = "Drelixgaming@gmail.com",
                Context = "Myself",
            };

            // Create
            personRepo.Create(person);

            person.PhoneNumbers = new List<PhoneNumber>()
            {
                new PhoneNumber() {Usage = "Work"},
                new PhoneNumber() {Usage = "School"}
            };

            // Read
            person = personRepo.Read(person.Id);

            Console.WriteLine(person.FirstName);

            // Update
            person.FirstName = "Karsten";

            personRepo.Update(person.Id, person);

            // Delete
            //personRepo.Delete(person);


        }
    }
}