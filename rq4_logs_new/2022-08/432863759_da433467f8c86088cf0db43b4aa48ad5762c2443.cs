using Data;
using Data.Entities;
using DataAccess.Concrete;
using Microsoft.EntityFrameworkCore;

namespace Test
{
    public class AgreementDalUnitTests
    {
        private readonly AppDbContext context;
        public AgreementDalUnitTests()
        {
            var optionsBuilder = new DbContextOptionsBuilder<AppDbContext>();
            optionsBuilder.UseInMemoryDatabase("Test");
            context = new AppDbContext(optionsBuilder.Options);
        }
        [Fact]
        public void GetModelById_WhenCalledById_ThenReturnsTrueAgreementModel()
        {
            // Assign
            var data = new List<Agreement>
            {
                new Agreement { Id = 1, Title = "False Agreement", Content = "Content 1" },
                new Agreement { Id = 2, Title = "True Agreement", Content = "Content 2" },
                new Agreement { Id = 3, Title = "False Agreement", Content = "Content 3" },
            };

            context.Agreements.AddRange(data);
            context.SaveChanges();

            var agreementDal = new AgreementDal(context);

            /// Act
            var result = agreementDal.GetModelById(2).Result;

            /// Assert
            Assert.Equal(2, result.Id);
            Assert.Equal("True Agreement", result.Title);
        }
    }
}