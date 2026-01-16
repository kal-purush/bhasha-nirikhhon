using Catcheap.API.Models.MiscModels;
using Catcheap.API.Services.MiscServices;
using Catcheap.Tests.CatcheapApi.MockRepo;

namespace Catcheap.Tests.CatcheapApi
{
    public class ChargingStationsServiceTest
    {
        [Fact]
        public void FilterChargingStationsByCityTest()
        {
            ChargingStationsService service = new ChargingStationsService(new MockChargingStationRepository());
            List<ChargingStation> list = service.FilterChargingStationsByCity("Vilnius").ToList();

            Assert.Equal(69, list[0].Id);
        }
    }
}