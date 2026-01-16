using Catcheap.API.Services.CarServices;
using Catcheap.Tests.CatcheapApi.MockRepo;


namespace Catcheap.Tests.CatcheapApi
{
    public class CarStatsServiceTest
    {
        [Fact]
        public void GetChargesCountSinceTest()
        {

            CarStatsService service = new CarStatsService(new MockCarRepository(), new MockCarChargeRepository(), new MockCarJourneyRepository());

            Double rez = service.GetChargesCountSince(69, new DateTime(2020,12,12,0,0,0));

            Assert.Equal(1, rez);
        }

        [Fact]
        public void GetJourneysCountSinceTest()
        {

            CarStatsService service = new CarStatsService(new MockCarRepository(), new MockCarChargeRepository(), new MockCarJourneyRepository());

            Double rez = service.GetJourneysCountSince(69, new DateTime(2020, 12, 12, 0, 0, 0));

            Assert.Equal(1, rez);
        }

        [Fact]
        public void GetJourneyDistanceSinceTest()
        {

            CarStatsService service = new CarStatsService(new MockCarRepository(), new MockCarChargeRepository(), new MockCarJourneyRepository());

            Double rez = service.GetJourneyDistanceSince(69, new DateTime(2020, 12, 12, 0, 0, 0));

            Assert.Equal(100, rez);
        }

        [Fact]
        public void GetConsumptionSinceTest()
        {

            CarStatsService service = new CarStatsService(new MockCarRepository(), new MockCarChargeRepository(), new MockCarJourneyRepository());

            Double rez = service.GetConsumptionSince(69, new DateTime(2020, 12, 12, 0, 0, 0));

            Assert.Equal(10, rez);
        }

        [Fact]
        public void GetMoneySpentSinceTest()
        {

            CarStatsService service = new CarStatsService(new MockCarRepository(), new MockCarChargeRepository(), new MockCarJourneyRepository());

            Double rez = service.GetMoneySpentSince(69, new DateTime(2020, 12, 12, 0, 0, 0));

            Assert.Equal(10, rez);
        }
    }
}