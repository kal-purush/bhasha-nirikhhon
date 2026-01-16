using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using CentralDeErros.Data.Context;
using CentralDeErros.Domain.Models;
using CentralDeErros.Domain.Test.Models;
using Microsoft.EntityFrameworkCore;
using Newtonsoft.Json;

namespace CentralDeErros.Domain.Test.Models.Base
{
    public class FakeContext
    {
        public DbContextOptions<CentralDeErrosContext> FakeOptions { get; }

        private Dictionary<Type, string> DataFileNames { get; } =
            new Dictionary<Type, string>();
        private string FileName<T>() { return DataFileNames[typeof(T)]; }

        public FakeContext(string testName)
        {
            FakeOptions = new DbContextOptionsBuilder<CentralDeErrosContext>()
                .UseInMemoryDatabase(databaseName: $"CentraDeErros_{testName}")
                .Options;

            DataFileNames.Add(typeof(ErrorLog), "FakeData\\ErrorLog.json");

        }

        public void FillWithAll()
        {
            FillWith<ErrorLog>();
        }

        public void FillWith<T>() where T : class
        {
            using (var context = new CentralDeErrosContext(FakeOptions))
            {
                if (context.Set<T>().Count() == 0)
                {
                    foreach (T item in GetFakeData<T>())
                        context.Set<T>().Add(item);
                    context.SaveChanges();
                }
            }
        }

        public List<T> GetFakeData<T>()
        {
            string content = File.ReadAllText(FileName<T>());
            return JsonConvert.DeserializeObject<List<T>>(content);
        }

    }
}