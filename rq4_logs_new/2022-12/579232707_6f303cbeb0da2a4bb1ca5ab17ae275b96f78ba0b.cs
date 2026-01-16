using Catcheap.API.Models.CarModels;
using Catcheap.API.Services.CarServices;
using Catcheap.Tests.CatcheapApi.MockRepo;

namespace Catcheap.Tests.CatcheapApi
{
    public class CarChargeServiceTest
    {

        [Fact]
        public void CalculateDurationOfChargeTest()
        {
            CarCharge charge = new CarCharge();
            charge.StartOfCharge = new DateTime(2022,12,18,12,0,0);
            charge.EndOfCharge = new DateTime(2022,12,18,15,30,0);
            CarChargeService service= new CarChargeService(new MockCarChargeRepository(), new MockNordpoolPriceRepository());
            TimeSpan rez = service.CalculateDurationOfCharge(charge);
            Assert.True( rez == (new TimeSpan(3,30,0)), "Bad");
        }

    }
}