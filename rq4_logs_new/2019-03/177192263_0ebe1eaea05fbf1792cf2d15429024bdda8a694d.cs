using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using test.Data;

namespace test
{
    public class AddAppConfig
    {
        public static async Task InitializAppConfig(ApplicationDbContext applicationDbContext)
        {
            List<AppConfig> Seting = new List<AppConfig>();
            Seting.Add(new AppConfig { Key = "SendEmail", Value = "Требуется заполнить" });
            Seting.Add(new AppConfig { Key = "EmailPassword", Value = "Требуется заполнить" });
            Seting.Add(new AppConfig { Key = "host", Value = "Требуется заполнить" });
            Seting.Add(new AppConfig { Key = "port", Value = "Требуется заполнить" });
            Seting.Add(new AppConfig { Key = "useSsl", Value = "Требуется заполнить" });

            foreach (var i in Seting)
                if (applicationDbContext.AppConfig.Where(p => p.Key == i.Key).FirstOrDefault() == null)
                    applicationDbContext.AppConfig.AddAsync(i);
                    applicationDbContext.SaveChanges();
        }
    }
}