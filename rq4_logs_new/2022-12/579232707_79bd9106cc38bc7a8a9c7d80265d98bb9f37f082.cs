using Catcheap.API.Data;
using Catcheap.API.Models.MiscModels;
using Catcheap.API.Repositories.CarRepo;
using Catcheap.API.Repositories.MiscRepo;
using Microsoft.EntityFrameworkCore;
using Moq;

namespace Catcheap.Tests.CatcheapApi.RepoTests
{
    public class ChargingStationRepositoryTest
    {
        [Fact]
        public void ChargingStationExistsTest()
        {
            List<ChargingStation> stations = new List<ChargingStation>();
            ChargingStation station = new ChargingStation();
            station.Id = 69;
            stations.Add(station);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.ChargingStations = DbContextMock.GetQueryableMockDbSet(stations);

            ChargingStationRepository repo = new ChargingStationRepository(myDbMoq.Object);

            Assert.True(repo.ChargingStationExists(69));
        }

        [Fact]
        public void CreateChargingStationTest()
        {
            List<ChargingStation> stations = new List<ChargingStation>();
            ChargingStation station = new ChargingStation();
            station.Id = 69;
            stations.Add(station);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.ChargingStations = DbContextMock.GetQueryableMockDbSet(stations);

            ChargingStationRepository repo = new ChargingStationRepository(myDbMoq.Object);

            Assert.True(repo.CreateChargingStation(station));
        }

        [Fact]
        public void DeleteChargingStationTest()
        {
            List<ChargingStation> stations = new List<ChargingStation>();
            ChargingStation station = new ChargingStation();
            station.Id = 69;
            stations.Add(station);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.ChargingStations = DbContextMock.GetQueryableMockDbSet(stations);

            ChargingStationRepository repo = new ChargingStationRepository(myDbMoq.Object);

            Assert.True(repo.DeleteChargingStation(station));
        }

        [Fact]
        public void GetChargingStationTest()
        {
            List<ChargingStation> stations = new List<ChargingStation>();
            ChargingStation station = new ChargingStation();
            station.Id = 69;
            stations.Add(station);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.ChargingStations = DbContextMock.GetQueryableMockDbSet(stations);

            ChargingStationRepository repo = new ChargingStationRepository(myDbMoq.Object);

            Assert.Equal(station,repo.GetChargingStation(69));
        }

        [Fact]
        public void GetChargingStationsTest()
        {
            List<ChargingStation> stations = new List<ChargingStation>();
            ChargingStation station = new ChargingStation();
            station.Id = 69;
            stations.Add(station);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.ChargingStations = DbContextMock.GetQueryableMockDbSet(stations);

            ChargingStationRepository repo = new ChargingStationRepository(myDbMoq.Object);

            Assert.Equal(stations, repo.GetChargingStations());
        }

        [Fact]
        public void SaveTest()
        {
            List<ChargingStation> stations = new List<ChargingStation>();
            ChargingStation station = new ChargingStation();
            station.Id = 69;
            stations.Add(station);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.ChargingStations = DbContextMock.GetQueryableMockDbSet(stations);

            ChargingStationRepository repo = new ChargingStationRepository(myDbMoq.Object);

            Assert.True(repo.Save());
        }

        [Fact]
        public void UpdateChargingStationTest()
        {
            List<ChargingStation> stations = new List<ChargingStation>();
            ChargingStation station = new ChargingStation();
            station.Id = 69;
            stations.Add(station);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.ChargingStations = DbContextMock.GetQueryableMockDbSet(stations);

            ChargingStationRepository repo = new ChargingStationRepository(myDbMoq.Object);

            Assert.True(repo.UpdateChargingStation(station));
        }
    }
}