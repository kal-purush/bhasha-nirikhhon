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
    public class ScooterChargeRepositoryTest
    {
        [Fact]
        public void ScooterChargeExistsTest()
        {
            List<ScooterCharge> charges = new List<ScooterCharge>();
            ScooterCharge charge = new ScooterCharge();
            charge.Id = 69;
            charges.Add(charge);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.ScooterCharges = DbContextMock.GetQueryableMockDbSet(charges);

            ScooterChargeRepository repo = new ScooterChargeRepository(myDbMoq.Object);

            Assert.True(repo.ScooterChargeExists(69));
        }

        [Fact]
        public void CreateScooterChargeTest()
        {
            List<ScooterCharge> charges = new List<ScooterCharge>();
            ScooterCharge charge = new ScooterCharge();
            charge.Id = 69;
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.ScooterCharges = DbContextMock.GetQueryableMockDbSet(charges);

            ScooterChargeRepository repo = new ScooterChargeRepository(myDbMoq.Object);

            Assert.True(repo.CreateScooterCharge(charge));
        }

        [Fact]
        public void DeleteScooterChargeTest()
        {
            List<ScooterCharge> charges = new List<ScooterCharge>();
            ScooterCharge charge = new ScooterCharge();
            charge.Id = 69;
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.ScooterCharges = DbContextMock.GetQueryableMockDbSet(charges);

            ScooterChargeRepository repo = new ScooterChargeRepository(myDbMoq.Object);

            Assert.True(repo.DeleteScooterCharge(charge));
        }

        [Fact]
        public void DeleteScooterChargesTest()
        {
            List<ScooterCharge> charges = new List<ScooterCharge>();
            ScooterCharge charge = new ScooterCharge();
            charge.Id = 69;
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.ScooterCharges = DbContextMock.GetQueryableMockDbSet(charges);

            ScooterChargeRepository repo = new ScooterChargeRepository(myDbMoq.Object);

            Assert.True(repo.DeleteScooterCharges(charges));
        }

        [Fact]
        public void GetScooterChargeTest()
        {
            List<ScooterCharge> charges = new List<ScooterCharge>();
            ScooterCharge charge = new ScooterCharge();
            charge.Id = 69;
            charges.Add(charge);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.ScooterCharges = DbContextMock.GetQueryableMockDbSet(charges);

            ScooterChargeRepository repo = new ScooterChargeRepository(myDbMoq.Object);

            Assert.Equal(charge, repo.GetScooterCharge(69));
        }

        [Fact]
        public void GetScooterChargesTest()
        {
            List<ScooterCharge> charges = new List<ScooterCharge>();
            ScooterCharge charge = new ScooterCharge();
            charge.Id = 69;
            charge.EndOfCharge = DateTime.Now;
            charges.Add(charge);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.ScooterCharges = DbContextMock.GetQueryableMockDbSet(charges);

            ScooterChargeRepository repo = new ScooterChargeRepository(myDbMoq.Object);

            Assert.Equal(charges, repo.GetScooterCharges());
        }

        [Fact]
        public void GetChargesOfAScooterTest()
        {
            List<ScooterCharge> charges = new List<ScooterCharge>();
            ScooterCharge charge = new ScooterCharge();
            charge.Id = 69;
            charge.Scooter = new Scooter();
            charge.Scooter.Id = 10;
            charges.Add(charge);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.ScooterCharges = DbContextMock.GetQueryableMockDbSet(charges);

            ScooterChargeRepository repo = new ScooterChargeRepository(myDbMoq.Object);

            Assert.Equal(charges, repo.GetChargesOfAScooter(10));
        }

        [Fact]
        public void SaveTest()
        {
            List<ScooterCharge> charges = new List<ScooterCharge>();
            ScooterCharge charge = new ScooterCharge();
            charge.Id = 69;
            charge.Scooter = new Scooter();
            charge.Scooter.Id = 10;
            charges.Add(charge);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.ScooterCharges = DbContextMock.GetQueryableMockDbSet(charges);

            ScooterChargeRepository repo = new ScooterChargeRepository(myDbMoq.Object);

            Assert.True(repo.Save());
        }

        [Fact]
        public void UpdateCarChargeTest()
        {
            List<ScooterCharge> charges = new List<ScooterCharge>();
            ScooterCharge charge = new ScooterCharge();
            charge.Id = 69;
            charge.Scooter = new Scooter();
            charge.Scooter.Id = 10;
            charges.Add(charge);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.ScooterCharges = DbContextMock.GetQueryableMockDbSet(charges);

            ScooterChargeRepository repo = new ScooterChargeRepository(myDbMoq.Object);

            Assert.True(repo.UpdateScooterCharge(charge));
        }
    }
}