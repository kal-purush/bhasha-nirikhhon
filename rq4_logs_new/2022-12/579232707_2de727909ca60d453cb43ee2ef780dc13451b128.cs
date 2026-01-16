using Catcheap.API.Data;
using Catcheap.API.Models.CarModels;
using Catcheap.API.Repositories.CarRepo;
using Microsoft.EntityFrameworkCore;
using Moq;


namespace Catcheap.Tests.CatcheapApi.RepoTests
{
    public class CarJourneyRepositoryTest
    {
        [Fact]
        public void CarJourneyExistsTest()
        {
            List<CarJourney> journeys = new List<CarJourney>();
            CarJourney journey = new CarJourney();
            journey.Id = 69;
            journeys.Add(journey);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.CarJourneys = DbContextMock.GetQueryableMockDbSet(journeys);

            CarJourneyRepository repo = new CarJourneyRepository(myDbMoq.Object);

            Assert.True(repo.CarJourneyExists(69));
        }

        [Fact]
        public void CreateCarJourneyTest()
        {
            List<CarJourney> journeys = new List<CarJourney>();
            CarJourney journey = new CarJourney();
            journey.Id = 69;
            journeys.Add(journey);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.CarJourneys = DbContextMock.GetQueryableMockDbSet(journeys);

            CarJourneyRepository repo = new CarJourneyRepository(myDbMoq.Object);

            Assert.True(repo.CreateCarJourney(journey));
        }

        [Fact]
        public void DeleteCarJourney()
        {
            List<CarJourney> journeys = new List<CarJourney>();
            CarJourney journey = new CarJourney();
            journey.Id = 69;
            journeys.Add(journey);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.CarJourneys = DbContextMock.GetQueryableMockDbSet(journeys);

            CarJourneyRepository repo = new CarJourneyRepository(myDbMoq.Object);

            Assert.True(repo.DeleteCarJourney(journey));
        }

        [Fact]
        public void DeleteCarJourneys()
        {
            List<CarJourney> journeys = new List<CarJourney>();
            CarJourney journey = new CarJourney();
            journey.Id = 69;
            journeys.Add(journey);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.CarJourneys = DbContextMock.GetQueryableMockDbSet(journeys);

            CarJourneyRepository repo = new CarJourneyRepository(myDbMoq.Object);

            Assert.True(repo.DeleteCarJourneys(journeys));
        }

        [Fact]
        public void GetCarJourneyTest()
        {
            List<CarJourney> journeys = new List<CarJourney>();
            CarJourney journey = new CarJourney();
            journey.Id = 69;
            journeys.Add(journey);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.CarJourneys = DbContextMock.GetQueryableMockDbSet(journeys);

            CarJourneyRepository repo = new CarJourneyRepository(myDbMoq.Object);

            Assert.Equal(journey,repo.GetCarJourney(69));
        }

        [Fact]
        public void GetCarJourneysTest()
        {
            List<CarJourney> journeys = new List<CarJourney>();
            CarJourney journey = new CarJourney();
            journey.Id = 69;
            journey.Date = DateTime.Now;
            journeys.Add(journey);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.CarJourneys = DbContextMock.GetQueryableMockDbSet(journeys);

            CarJourneyRepository repo = new CarJourneyRepository(myDbMoq.Object);

            Assert.Equal(journeys, repo.GetCarJourneys());
        }

        [Fact]
        public void GetJourneysOfACarTest()
        {
            List<CarJourney> journeys = new List<CarJourney>();
            CarJourney journey = new CarJourney();
            journey.Id = 69;
            journey.Car = new Car();
            journey.Car.Id = 10;
            journeys.Add(journey);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.CarJourneys = DbContextMock.GetQueryableMockDbSet(journeys);

            CarJourneyRepository repo = new CarJourneyRepository(myDbMoq.Object);

            Assert.Equal(journeys, repo.GetJourneysOfACar(10));
        }

        [Fact]
        public void SaveTest()
        {
            List<CarJourney> journeys = new List<CarJourney>();
            CarJourney journey = new CarJourney();
            journey.Id = 69;
            journeys.Add(journey);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.CarJourneys = DbContextMock.GetQueryableMockDbSet(journeys);

            CarJourneyRepository repo = new CarJourneyRepository(myDbMoq.Object);

            Assert.True(repo.Save());
        }

        [Fact]
        public void UpdateCarJourneyTest()
        {
            List<CarJourney> journeys = new List<CarJourney>();
            CarJourney journey = new CarJourney();
            journey.Id = 69;
            journeys.Add(journey);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.CarJourneys = DbContextMock.GetQueryableMockDbSet(journeys);

            CarJourneyRepository repo = new CarJourneyRepository(myDbMoq.Object);

            Assert.True(repo.UpdateCarJourney(journey));
        }
    }
}