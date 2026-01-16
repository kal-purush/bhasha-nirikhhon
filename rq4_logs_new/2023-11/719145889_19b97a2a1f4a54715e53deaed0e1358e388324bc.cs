using Bogus;
using legallead.jdbc.entities;

namespace legallead.jdbc.tests.entities
{
    public class PermissionMapTests
    {
        private readonly Faker<PermissionMap> faker =
            new Faker<PermissionMap>()
            .RuleFor(x => x.Id, y => y.Random.Guid().ToString("D"))
            .RuleFor(x => x.OrderId, y => y.Random.Int(1, 10000))
            .RuleFor(x => x.KeyName, y => y.Company.CompanyName());

        [Fact]
        public void PermissionMapCanBeCreated()
        {
            var exception = Record.Exception(() =>
            {
                _ = new PermissionMap();
            });
            Assert.Null(exception);
        }

        [Fact]
        public void PermissionMapCanUpdateId()
        {
            var items = faker.Generate(2);
            items[0].Id = items[1].Id;
            Assert.Equal(items[1].Id, items[0].Id);
        }

        [Fact]
        public void PermissionMapCanUpdateOrderId()
        {
            var items = faker.Generate(2);
            items[0].OrderId = items[1].OrderId;
            Assert.Equal(items[1].OrderId, items[0].OrderId);
        }

        [Fact]
        public void PermissionMapCanUpdateKeyName()
        {
            var items = faker.Generate(2);
            items[0].KeyName = items[1].KeyName;
            Assert.Equal(items[1].KeyName, items[0].KeyName);
        }
    }
}