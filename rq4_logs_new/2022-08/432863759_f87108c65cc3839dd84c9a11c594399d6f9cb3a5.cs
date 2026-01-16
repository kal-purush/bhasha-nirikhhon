using Data;
using Data.Entities;
using DataAccess.Concrete;
using Microsoft.EntityFrameworkCore;

namespace Test
{
    public class UserDalUnitTests
    {
        private readonly AppDbContext context;
        public UserDalUnitTests()
        {
            var optionsBuilder = new DbContextOptionsBuilder<AppDbContext>();
            optionsBuilder.UseInMemoryDatabase("Test");
            context = new AppDbContext(optionsBuilder.Options);
        }
        [Fact]
        public void GetByMail_WhenUserCalledByEmail_ThenReturnsUser()
        {
            // Assign
            var data = new List<User>
            {
                new User { Id = 1, Email = "myemail@gmail.com", Name = "Muhsin", Job = "Job" },
                new User { Id = 2, Email = "myemail2@gmail.com", Name = "Muhsin2", Job = "Job2" },
                new User { Id = 3, Email = "myemail3@gmail.com", Name = "Muhsin3", Job = "Job3" },
            };

            context.Users.AddRange(data);
            context.SaveChanges();

            var userDal = new UserDal(context);

            /// Act
            var result = userDal.GetByMail("myemail2@gmail.com").Result;

            /// Assert
            Assert.Equal(2, result.Id);
            Assert.Equal("Muhsin2", result.Name);
            Assert.Equal("myemail2@gmail.com", result.Email);
        }
    }
}