using System;
using System.Linq;

namespace Tapir.Models
{
    public class DbInitializer
    {
        public static void Initialize(TapirContext context)
        {
            if (context.Company.Any())
            {
                return;
            }

            var companies = new Company[]
            {
                new Company{FullName="Osakeyhtiö 1 Oy", ShortName="Oy1"},
                new Company{FullName="Osakeyhtiö 2 Oy", ShortName="Oy2"},
                new Company{FullName="Osakeyhtiö 3 Oy", ShortName="Oy3"}
            };
            foreach (Company c in companies)
            {
                context.Company.Add(c);
            }
            context.SaveChanges();

            var persons = new Person[]
            {
                new Person{LastName="Avanne", FirstNames="Aarne Aatami", DOB=new DateTime(1960, 6, 30)},
                new Person{LastName="Bergström", FirstNames="Bertta Berit", DOB=new DateTime(1971, 12, 31)},
                new Person{LastName="Cronqvist", FirstNames="Cecilia Cella", DOB=new DateTime(1982, 1, 13)}
            };
            foreach (Person p in persons)
            {
                context.Person.Add(p);
            }
            context.SaveChanges();

            var employments = new Employment[]
            {
                new Employment{
                    PersonID = persons.Single(p => p.LastName == "Avanne").ID,
                    CompanyID = companies.Single(c => c.ShortName == "Oy1").ID
                },
                new Employment{
                    PersonID = persons.Single(p => p.LastName == "Bergström").ID,
                    CompanyID = companies.Single(c => c.ShortName == "Oy1").ID
                },
                new Employment{
                    PersonID = persons.Single(p => p.LastName == "Bergström").ID,
                    CompanyID = companies.Single(c => c.ShortName == "Oy2").ID
                },
                new Employment{
                    PersonID = persons.Single(p => p.LastName == "Cronqvist").ID,
                    CompanyID = companies.Single(c => c.ShortName == "Oy2").ID
                },
            };
            foreach (Employment e in employments)
            {
                context.Employment.Add(e);
            }
            context.SaveChanges();
        }
    }
}