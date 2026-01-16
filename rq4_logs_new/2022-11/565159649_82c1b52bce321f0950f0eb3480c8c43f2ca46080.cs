using CleanArchMvc.Domain.Entities;
using CleanArchMvc.Infra.Data.Context;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Linq;

namespace CleanArchMvc.Infra.Data
{
    public static class SeedData
    {
        public static void Seed(this IServiceProvider services)
        {
            var scope = services.CreateScope();

            var context = scope.ServiceProvider.GetService<ApplicationDbContext>();

            var transaction = context.Database.BeginTransaction();

            if (!context.Categories.Any())
            {
                context.Categories.Add(new Category(1, "Cars"));
                context.Categories.Add(new Category(2, "Motorcycles"));
                context.Categories.Add(new Category(3, "Bicycles"));
                context.Categories.Add(new Category(4, "Eletronics"));
            }

            if (!context.Products.Any())
            {
                context.Products.Add(new Product("Fiat", "A beautiful car for your family", 12000m, 3, null)
                {
                    CategoryId = 1,
                });
                context.Products.Add(new Product("XJ 6", "Do you love run?", 36000m, 3, null)
                {
                    CategoryId = 2,
                });
                context.Products.Add(new Product("Bicycle Aro 29", "Bicycle Aro 29 KRW Aluminum Shimano TZ 24 Vel Disc Brake Ltx S40 - Blue+Black", 993.52m, 3, null)
                {
                    CategoryId = 3,
                });
                context.Products.Add(new Product("Head Phone", "Listen your favorite musics", 10m, 50, null)
                {
                    CategoryId = 4,
                });
                context.Products.Add(new Product("IPhone Max Pro B X Vw 556 master plus mega fucker", "A simple phone", 100_000m, 53, null)
                {
                    CategoryId = 4,
                });
            }

            try
            {
                context.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Categories ON;");

                context.SaveChanges();

                context.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Categories OFF;");

                transaction.Commit();
            }
            catch
            {
                transaction.Rollback();

                throw;
            }
        }
    }
}