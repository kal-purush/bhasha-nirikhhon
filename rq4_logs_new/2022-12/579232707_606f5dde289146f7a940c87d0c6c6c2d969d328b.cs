using Catcheap.API.Models.MiscModels;
using Catcheap.API.Services.NordpoolPriceServices;
using Catcheap.Tests.CatcheapApi.MockRepo;

namespace Catcheap.Tests.CatcheapApi
{
    public class NordpoolPriceServiceTest
    {
        [Fact]
        public void GetNordpoolPriceCheapestSinceTest()
        {
            NordpoolPriceService service = new NordpoolPriceService(new MockNordpoolPriceRepository());

            NordpoolPrice price = service.GetNordpoolPriceCheapestSince(new DateTime(2020, 12, 12, 12, 0, 0));

            Assert.Equal(69, price.Id);
        }

        [Fact]
        public void GetNordpoolPriceMostExpensiceSinceTest()
        {
            NordpoolPriceService service = new NordpoolPriceService(new MockNordpoolPriceRepository());

            NordpoolPrice price = service.GetNordpoolPriceMostExpensiceSince(new DateTime(2020, 12, 12, 12, 0, 0));

            Assert.Equal(420, price.Id);
        }
        [Fact]
        public void GetNordpoolPricesAverageSinceTest()
        {
            NordpoolPriceService service = new NordpoolPriceService(new MockNordpoolPriceRepository());

            double rez = service.GetNordpoolPricesAverageSince(new DateTime(2020, 12, 12, 12, 0, 0));

            Assert.Equal(5.5, rez);
        }
    }
}