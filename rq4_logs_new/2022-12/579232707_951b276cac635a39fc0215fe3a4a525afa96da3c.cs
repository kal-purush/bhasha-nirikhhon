using Catcheap.API.Controllers.CarControl;
using Catcheap.API.Controllers.ScooterControl;
using Catcheap.API.Services.ScooterServices;
using Catcheap.Tests.CatcheapApi.MockRepo;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Catcheap.Tests.CatcheapApi.ControllerTests
{
    public class ScooterStatsControllerTest
    {
        [Fact]
        public void GetScooterChargesCountTest()
        {

            ScooterStatsController controller = new ScooterStatsController(new MockScooterRepository(),
                    new ScooterStatsService(new MockScooterRepository(), new MockScooterChargeRepository(), new MockScooterJourneyRepository()));

            var respons = (OkObjectResult)controller.GetScooterChargesCount(69, new DateTime(2020, 12, 12, 12, 0, 0));
            var rez = respons.Value;

            double rez1 = (double)rez;

            Assert.Equal((double)1, rez);

        }

        [Fact]
        public void GetScooterJourneysCountTest()
        {

            ScooterStatsController controller = new ScooterStatsController(new MockScooterRepository(),
                    new ScooterStatsService(new MockScooterRepository(), new MockScooterChargeRepository(), new MockScooterJourneyRepository()));

            var respons = (OkObjectResult)controller.GetScooterJourneysCount(69, new DateTime(2020, 12, 12, 12, 0, 0));
            var rez = respons.Value;

            double rez1 = (double)rez;

            Assert.Equal((double)1, rez);

        }

        [Fact]
        public void GetScooterJourneyDistanceTest()
        {

            ScooterStatsController controller = new ScooterStatsController(new MockScooterRepository(),
                    new ScooterStatsService(new MockScooterRepository(), new MockScooterChargeRepository(), new MockScooterJourneyRepository()));

            var respons = (OkObjectResult)controller.GetScooterJourneyDistance(69, new DateTime(2020, 12, 12, 12, 0, 0));
            var rez = respons.Value;

            double rez1 = (double)rez;

            Assert.Equal((double)100, rez);

        }

        [Fact]
        public void GetScooterConsumptionTest()
        {

            ScooterStatsController controller = new ScooterStatsController(new MockScooterRepository(),
                    new ScooterStatsService(new MockScooterRepository(), new MockScooterChargeRepository(), new MockScooterJourneyRepository()));

            var respons = (OkObjectResult)controller.GetScooterConsumption(69, new DateTime(2020, 12, 12, 12, 0, 0));
            var rez = respons.Value;

            double rez1 = (double)rez;

            Assert.Equal((double)10, rez);

        }

        [Fact]
        public void GetScooterMoneySpentTest()
        {

            ScooterStatsController controller = new ScooterStatsController(new MockScooterRepository(),
                    new ScooterStatsService(new MockScooterRepository(), new MockScooterChargeRepository(), new MockScooterJourneyRepository()));

            var respons = (OkObjectResult)controller.GetCarMoneySpent(69, new DateTime(2020, 12, 12, 12, 0, 0));
            var rez = respons.Value;

            double rez1 = (double)rez;

            Assert.Equal((double)10, rez);

        }
    }
}