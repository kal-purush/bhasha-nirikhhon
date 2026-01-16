using Microsoft.AspNetCore.Identity;
using Microsoft.Extensions.DependencyInjection;
using StudioModel.Domain;

namespace StudioDataAccess.Seed
{
    public static class DatabaseSeed
    {
        public static UserManager<UserApp> Manager { get; set; }

        private const string Admin = "admin";
        private const string TestUser = "user";
        private const string TestTeacher = "teacher";
        private const string CommonPassword = "Password123!";
        private const string AdminPassword = "AdminPassword123!";
        public static async Task SeedUsersAsync(IServiceProvider serviceProvider)
        {
           Manager = serviceProvider.GetRequiredService<UserManager<UserApp>>();

            if(await findUserExist(Admin))
            {
                var adminUser = new UserApp { UserName = Admin, Email = "admin@pegasus.net" };
                await Manager.CreateAsync(adminUser, AdminPassword);
            }

            if (await findUserExist(TestUser))
            {
                var testUser = new UserApp { UserName = TestUser, Email = "testuser@pegasus.net" };
                await Manager.CreateAsync(testUser, CommonPassword);
            }

            if (await findUserExist(TestTeacher))
            {
                var testTeacher = new UserApp { UserName = TestTeacher, Email = "testteacher@pegasus.net" };
                await Manager.CreateAsync(testTeacher, CommonPassword);
            }
        }

        private static async Task<bool> findUserExist(string name)
        {
            return await Manager.FindByNameAsync(name) == null;
        }
    }
}