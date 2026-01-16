using System;
using Api.Data.Context;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Xunit;


namespace Api.Data.Test
{
    public abstract class BaseTest
    {
        public BaseTest()
        {
            
        }
    }

    public class DbTest : IDisposable
    {
        private string  dataBaseName = $"dbApiTest_{Guid.NewGuid().ToString().Replace("-", string.Empty)}";
        public ServiceProvider serviceProvider {get; private set;}

        public DbTest() 
        {
            var serviceColllection = new ServiceCollection();
            serviceColllection.AddDbContext<MyContext>(o => 
                o.UseSqlServer($"Persist Security Info=True;Server=DESKTOP-1DF7BFU;Initial Catalog={dataBaseName};MultipleActiveResultSets=true;User ID=sa;Password=lbr@2020"),
                ServiceLifetime.Transient
            );

            serviceProvider = serviceProvider.BuilderServicesProv();
            using(var context = ServiceProvider.GetService<MyContext>())
            {
                context.Database.EnsureCreated();
            }
        }

        public void Dispose() 
        {
            using(var context = ServiceProvider.GetService<MyContext>())
            {
                context.Database.EnsureCreated();
            }
        }
    }
}