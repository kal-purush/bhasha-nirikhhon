using Gride.Models;
using System;
using System.Linq;

namespace Gride.Data
{
    public static class DbInitializer
    {
        public static void Initialize(ApplicationDbContext context)
        {
            context.Database.EnsureCreated();

            // Look for any students.
            if (context.EmployeeModel.Any())
            {
                return;   // DB has been seeded
            }

            var employees = new EmployeeModel[]
            {
                new EmployeeModel{
                    ID=1,
                    Name ="Guus",
                    LastName ="Joppe",
                    DoB =DateTime.Parse("1998-05-18"),
                    Gender =0,
                    EMail ="0967844@hr.nl",
                    PhoneNumber ="0640643724",
                    Admin =true,
                    LoginID =0967844,
                    Experience =1,
                    Locations =1,
                    ProfileImage ="profile_0967844.jpeg"
                },
                new EmployeeModel
                {
                    ID = 2,
                    Name = "John",
                    LastName = "Doe",
                    DoB = DateTime.Parse("1997-01-01"),
                    Gender = 0,
                    EMail = "0123456@hr.nl",
                    PhoneNumber = "0612345678",
                    Admin = false,
                    LoginID = 0123456,
                    Experience = 2,
                    Locations = 1,
                    ProfileImage = "profile_0123456.jpeg"
                }
            };
            foreach (EmployeeModel e in employees)
            {
                context.EmployeeModel.Add(e);
            }
            context.SaveChanges();

            var skills = new Skill[]
            {
                new Skill{Name="Nederlands",EmployeeModelID=1},
                new Skill{Name="Frans",EmployeeModelID=1},
                new Skill{Name="Engels",EmployeeModelID=1}
            };
            foreach (Skill s in skills)
            {
                context.Skill.Add(s);
            }
            context.SaveChanges();
        }
    }
}