using EasyMicroservices.TemplateGeneratorMicroservice.Database;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EasyMicroservices.TemplateGeneratorMicroservice
{
    public class DatabaseBuilder : IDatabaseBuilder
    {
        public void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseInMemoryDatabase("TemplateGenerator database");
            //optionsBuilder.UseSqlServer("Server=.;Database=TemplateGenerator;User ID=test;Password=test1234;Trusted_Connection=True;TrustServerCertificate=True");
        }
    }
}