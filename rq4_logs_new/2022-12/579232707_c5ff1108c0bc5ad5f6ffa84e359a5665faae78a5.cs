using Catcheap.API.Data;
using Catcheap.API.Models.ScooterModels;
using Catcheap.API.Repositories.ScooterRepo;
using Microsoft.EntityFrameworkCore;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Catcheap.Tests.CatcheapApi.RepoTests
{
    public class ScooterJourneyRepositoryTest
    {
        [Fact]
        public void ScooterJourneyExistsTest()
        {
            List<ScooterJourney> journeys = new List<ScooterJourney>();
            ScooterJourney journey = new ScooterJourney();
            journey.Id = 69;
            journeys.Add(journey);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.ScooterJourneys = DbContextMock.GetQueryableMockDbSet(journeys);

            ScooterJourneyRepository repo = new ScooterJourneyRepository(myDbMoq.Object);

            Assert.True(repo.ScooterJourneyExists(69));
        }

        [Fact]
        public void CreateScooterJourneyTest()
        {
            List<ScooterJourney> journeys = new List<ScooterJourney>();
            ScooterJourney journey = new ScooterJourney();
            journey.Id = 69;
            journeys.Add(journey);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.ScooterJourneys = DbContextMock.GetQueryableMockDbSet(journeys);

            ScooterJourneyRepository repo = new ScooterJourneyRepository(myDbMoq.Object);

            Assert.True(repo.CreateScooterJourney(journey));
        }

        [Fact]
        public void DeleteScooterJourney()
        {
            List<ScooterJourney> journeys = new List<ScooterJourney>();
            ScooterJourney journey = new ScooterJourney();
            journey.Id = 69;
            journeys.Add(journey);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.ScooterJourneys = DbContextMock.GetQueryableMockDbSet(journeys);

            ScooterJourneyRepository repo = new ScooterJourneyRepository(myDbMoq.Object);

            Assert.True(repo.DeleteScooterJourney(journey));
        }

        [Fact]
        public void DeleteScooterJourneys()
        {
            List<ScooterJourney> journeys = new List<ScooterJourney>();
            ScooterJourney journey = new ScooterJourney();
            journey.Id = 69;
            journeys.Add(journey);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.ScooterJourneys = DbContextMock.GetQueryableMockDbSet(journeys);

            ScooterJourneyRepository repo = new ScooterJourneyRepository(myDbMoq.Object);

            Assert.True(repo.DeleteScooterJourneys(journeys));
        }

        [Fact]
        public void GetScooterJourneyTest()
        {
            List<ScooterJourney> journeys = new List<ScooterJourney>();
            ScooterJourney journey = new ScooterJourney();
            journey.Id = 69;
            journeys.Add(journey);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.ScooterJourneys = DbContextMock.GetQueryableMockDbSet(journeys);

            ScooterJourneyRepository repo = new ScooterJourneyRepository(myDbMoq.Object);

            Assert.Equal(journey, repo.GetScooterJourney(69));
        }

        [Fact]
        public void GetScooterJourneysTest()
        {
            List<ScooterJourney> journeys = new List<ScooterJourney>();
            ScooterJourney journey = new ScooterJourney();
            journey.Id = 69;
            journey.Date = DateTime.Now;
            journeys.Add(journey);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.ScooterJourneys = DbContextMock.GetQueryableMockDbSet(journeys);

            ScooterJourneyRepository repo = new ScooterJourneyRepository(myDbMoq.Object);

            Assert.Equal(journeys, repo.GetScooterJourneys());
        }

        [Fact]
        public void GetJourneysOfAScooterTest()
        {
            List<ScooterJourney> journeys = new List<ScooterJourney>();
            ScooterJourney journey = new ScooterJourney();
            journey.Id = 69;
            journey.Scooter = new Scooter();
            journey.Scooter.Id = 10;
            journeys.Add(journey);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.ScooterJourneys = DbContextMock.GetQueryableMockDbSet(journeys);

            ScooterJourneyRepository repo = new ScooterJourneyRepository(myDbMoq.Object);

            Assert.Equal(journeys, repo.GetJourneysOfAScooter(10));
        }

        [Fact]
        public void SaveTest()
        {
            List<ScooterJourney> journeys = new List<ScooterJourney>();
            ScooterJourney journey = new ScooterJourney();
            journey.Id = 69;
            journeys.Add(journey);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.ScooterJourneys = DbContextMock.GetQueryableMockDbSet(journeys);

            ScooterJourneyRepository repo = new ScooterJourneyRepository(myDbMoq.Object);

            Assert.True(repo.Save());
        }

        [Fact]
        public void UpdateScooterJourneyTest()
        {
            List<ScooterJourney> journeys = new List<ScooterJourney>();
            ScooterJourney journey = new ScooterJourney();
            journey.Id = 69;
            journeys.Add(journey);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.ScooterJourneys = DbContextMock.GetQueryableMockDbSet(journeys);

            ScooterJourneyRepository repo = new ScooterJourneyRepository(myDbMoq.Object);

            Assert.True(repo.UpdateScooterJourney(journey));
        }
    }
}