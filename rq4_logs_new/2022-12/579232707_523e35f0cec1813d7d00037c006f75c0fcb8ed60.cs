using Catcheap.API.Controllers.CarControl;
using Catcheap.API.Services.CarServices;
using Catcheap.Tests.CatcheapApi.MockRepo;
using Microsoft.AspNetCore.Mvc;

namespace Catcheap.Tests.CatcheapApi.ControllerTests
{
    public class CarStatsControllerTest
    {
        [Fact]
        public void GetCarChargesCountTest()
        {

            CarStatsController controller = new CarStatsController(new MockCarRepository(),
                    new CarStatsService(new MockCarRepository(), new MockCarChargeRepository(), new MockCarJourneyRepository()));

            var respons = (OkObjectResult)controller.GetCarChargesCount(69, new DateTime(2020,12,12,12,0,0));
            var rez = respons.Value;

            double rez1 = (double)rez;

            Assert.Equal((double)1, rez);

        }

        [Fact]
        public void GetCarJourneysCountTest()
        {

            CarStatsController controller = new CarStatsController(new MockCarRepository(),
                    new CarStatsService(new MockCarRepository(), new MockCarChargeRepository(), new MockCarJourneyRepository()));

            var respons = (OkObjectResult)controller.GetCarJourneysCount(69, new DateTime(2020, 12, 12, 12, 0, 0));
            var rez = respons.Value;

            double rez1 = (double)rez;

            Assert.Equal((double)1, rez);

        }

        [Fact]
        public void GetCarJourneyDistanceTest()
        {

            CarStatsController controller = new CarStatsController(new MockCarRepository(),
                    new CarStatsService(new MockCarRepository(), new MockCarChargeRepository(), new MockCarJourneyRepository()));

            var respons = (OkObjectResult)controller.GetCarJourneyDistance(69, new DateTime(2020, 12, 12, 12, 0, 0));
            var rez = respons.Value;

            double rez1 = (double)rez;

            Assert.Equal((double)100, rez);

        }

        [Fact]
        public void GetCarConsumptionTest()
        {

            CarStatsController controller = new CarStatsController(new MockCarRepository(),
                    new CarStatsService(new MockCarRepository(), new MockCarChargeRepository(), new MockCarJourneyRepository()));

            var respons = (OkObjectResult)controller.GetCarConsumption(69, new DateTime(2020, 12, 12, 12, 0, 0));
            var rez = respons.Value;

            double rez1 = (double)rez;

            Assert.Equal((double)10, rez);

        }

        [Fact]
        public void GetCarMoneySpentTest()
        {

            CarStatsController controller = new CarStatsController(new MockCarRepository(),
                    new CarStatsService(new MockCarRepository(), new MockCarChargeRepository(), new MockCarJourneyRepository()));

            var respons = (OkObjectResult)controller.GetCarMoneySpent(69, new DateTime(2020, 12, 12, 12, 0, 0));
            var rez = respons.Value;

            double rez1 = (double)rez;

            Assert.Equal((double)10, rez);

        }

    }
}