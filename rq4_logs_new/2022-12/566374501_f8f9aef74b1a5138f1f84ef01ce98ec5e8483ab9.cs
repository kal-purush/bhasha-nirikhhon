using Microsoft.Extensions.Logging;
using Standard.Tool.Platform.Data.Entities;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Standard.Tool.Platform.Data;

public class Seed
{
    public static async Task SeedAsync(ToolsBlockDbContext dbContext, ILogger logger, int retry = 0)
    {
        var retryForAvailability = retry;

        try
        {
            await dbContext.Account.AddRangeAsync(GetAccounts());

            await dbContext.SaveChangesAsync();
        }
        catch (Exception e)
        {
            if (retryForAvailability >= 10) throw;

            retryForAvailability++;

            logger.LogError(e.Message);
            await SeedAsync(dbContext, logger, retryForAvailability);
            throw;
        }
    }

    private static IEnumerable<AccountEntity> GetAccounts()
    {
        return new List<AccountEntity>
    {
        new()
        {
            Id = Guid.Parse("ab78493d-7569-42d2-ae78-c2b610ada1aa"),
            UserName = "admin",
            PasswordHash = "JAvlGPq9JyTdtvBO6x2llnRI1+gxwIyPqCKAn3THIKk=",
            Status="Enable",
            CreateTimeUtc = DateTime.UtcNow
        }
    };
    }
}