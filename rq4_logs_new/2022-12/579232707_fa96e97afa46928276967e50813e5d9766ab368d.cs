using Catcheap.API.Services.ScooterServices;
using Catcheap.Tests.CatcheapApi.MockRepo;


namespace Catcheap.Tests.CatcheapApi
{
    public class ScooterStatsServiceTest
    {
        [Fact]
        public void GetChargesCountSinceTest()
        {

            ScooterStatsService service = new ScooterStatsService(new MockScooterRepository(), new MockScooterChargeRepository(), new MockScooterJourneyRepository());

            double rez = service.GetChargesCountSince(69, new DateTime(2020, 12, 12, 0, 0, 0));

            Assert.Equal(1, rez);
        }

        [Fact]
        public void GetJourneysCountSinceTest()
        {

            ScooterStatsService service = new ScooterStatsService(new MockScooterRepository(), new MockScooterChargeRepository(), new MockScooterJourneyRepository());

            double rez = service.GetJourneysCountSince(69, new DateTime(2020, 12, 12, 0, 0, 0));

            Assert.Equal(1, rez);
        }

        [Fact]
        public void GetJourneyDistanceSinceTest()
        {

            ScooterStatsService service = new ScooterStatsService(new MockScooterRepository(), new MockScooterChargeRepository(), new MockScooterJourneyRepository());

            double rez = service.GetJourneyDistanceSince(69, new DateTime(2020, 12, 12, 0, 0, 0));

            Assert.Equal(100, rez);
        }

        [Fact]
        public void GetConsumptionSinceTest()
        {

            ScooterStatsService service = new ScooterStatsService(new MockScooterRepository(), new MockScooterChargeRepository(), new MockScooterJourneyRepository());

            double rez = service.GetConsumptionSince(69, new DateTime(2020, 12, 12, 0, 0, 0));

            Assert.Equal(10, rez);
        }

        [Fact]
        public void GetMoneySpentSinceTest()
        {

            ScooterStatsService service = new ScooterStatsService(new MockScooterRepository(), new MockScooterChargeRepository(), new MockScooterJourneyRepository());

            double rez = service.GetMoneySpentSince(69, new DateTime(2020, 12, 12, 0, 0, 0));

            Assert.Equal(10, rez);
        }
    }
}