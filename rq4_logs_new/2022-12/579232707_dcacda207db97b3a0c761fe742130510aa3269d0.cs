using Catcheap.API.Controllers.MiscControl;
using Catcheap.API.Models.MiscModels;
using Catcheap.API.Services.MiscServices;
using Catcheap.Tests.CatcheapApi.MockRepo;
using Microsoft.AspNetCore.Mvc;

namespace Catcheap.Tests.CatcheapApi.ControllerTests
{
    public class ChargingStationControllerTest
    {
        [Fact]
        public void GetChargingStationsTest()
        {

            ChargingStationController controller = new ChargingStationController(new MockChargingStationRepository(),
                    new ChargingStationsService(new MockChargingStationRepository()));

            var respons = (OkObjectResult)controller.GetChargingStations();
            var rez = respons.Value;

            List<ChargingStation> l = (List<ChargingStation>)rez;
            Assert.Equal(69, l[0].Id);

        }

        [Fact]
        public void GetChargingStationTest()
        {

            ChargingStationController controller = new ChargingStationController(new MockChargingStationRepository(),
                    new ChargingStationsService(new MockChargingStationRepository()));

            var respons = (OkObjectResult)controller.GetChargingStation(69);
            var rez = respons.Value;

            ChargingStation l = (ChargingStation)rez;
            Assert.Equal(69, l.Id);

        }

        [Fact]
        public void CreateChargingStationTest()
        {

            ChargingStationController controller = new ChargingStationController(new MockChargingStationRepository(),
                    new ChargingStationsService(new MockChargingStationRepository()));

            ChargingStation station = new ChargingStation();
            station.Id = 68;
            station.City = "Kaunas";
            station.Street = "medis";
            station.StreetNumber = 2;
            var respons = (OkObjectResult)controller.CreateChargingStation(station);

            Assert.Equal(200, respons.StatusCode);

        }

        [Fact]
        public void UpdateChargingStationTest()
        {

            ChargingStationController controller = new ChargingStationController(new MockChargingStationRepository(),
                    new ChargingStationsService(new MockChargingStationRepository()));

            ChargingStation station = new ChargingStation();
            station.Id = 69;
            station.City = "Kaunas";
            station.Street = "medis";
            station.StreetNumber = 2;
            var respons = (NoContentResult)controller.UpdateChargingStation(69, station);

            Assert.Equal(204, respons.StatusCode);

        }

        [Fact]
        public void DeleteChargingStationTest()
        {

            ChargingStationController controller = new ChargingStationController(new MockChargingStationRepository(),
                    new ChargingStationsService(new MockChargingStationRepository()));

            var respons = (NoContentResult)controller.DeleteChargingStation(69);

            Assert.Equal(204, respons.StatusCode);

        }

        [Fact]
        public void GetChargingStationsCityTest()
        {

            ChargingStationController controller = new ChargingStationController(new MockChargingStationRepository(),
                    new ChargingStationsService(new MockChargingStationRepository()));

            var respons = (OkObjectResult)controller.GetChargingStations("Vilnius");
            var rez = respons.Value;

            List<ChargingStation> l = ((IEnumerable<ChargingStation>)rez).ToList();
            Assert.Equal(69, l[0].Id);

        }
    }
}