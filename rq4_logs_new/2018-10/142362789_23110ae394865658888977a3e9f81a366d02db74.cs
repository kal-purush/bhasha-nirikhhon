namespace UrbanEngine.Tests.Fixtures {
    using Microsoft.EntityFrameworkCore;
    using Infrastructure.Repository;
    using TestData;

    public class InMemoryRepositoryFixture {

        public InMemoryRepositoryFixture() {
            Repository = CreateRepository(); 
        }

        private IRepository CreateRepository() { 
            var builder = new DbContextOptionsBuilder<TestDbContext>();
            builder.UseInMemoryDatabase( "InMemoryDatabase" );
             
            TestDbContext dbContext = new TestDbContext( builder.Options );
            dbContext.Database.EnsureDeleted();
            dbContext.Database.EnsureCreated(); 

            return new Repository( dbContext ); 
        }
         
        public IRepository Repository { get; private set; } 
    } 
}