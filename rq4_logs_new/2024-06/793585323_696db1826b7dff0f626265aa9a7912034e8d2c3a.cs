using Persistence.DAL;

namespace UnitTesting.Persistence;

public class ResetSeeder
{
    private UpdateSeeder _updateSeeder;
    [OneTimeSetUp]
    public void SetUp()
    {
        _updateSeeder = new UpdateSeeder();
        _updateSeeder.PrepareDatabase(IntegrationMode.Integration);
    }

    [Test]
    public void Reset()
    {
        _updateSeeder.AddUsers();
        _updateSeeder.AddProducts();
        _updateSeeder.AddOrders();
    }
}