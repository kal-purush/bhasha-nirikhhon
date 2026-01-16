using Catcheap.API.Data;
using Catcheap.API.Models.CarModels;
using Catcheap.API.Models.ScooterModels;
using Catcheap.API.Repositories.CarRepo;
using Microsoft.EntityFrameworkCore;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Catcheap.Tests.CatcheapApi.RepoTests
{
    public class CarChargeRepositoryTest
    {
        
        [Fact]
        public void CarChargeExistsTest()
        {
            List<CarCharge> charges = new List<CarCharge>();
            CarCharge charge = new CarCharge();
            charge.Id = 69;
            charges.Add(charge);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.CarCharges = DbContextMock.GetQueryableMockDbSet(charges);

            CarChargeRepository repo = new CarChargeRepository(myDbMoq.Object);

            Assert.True(repo.CarChargeExists(69));
        }

        [Fact]
        public void CreateCarChargeTest()
        {
            List<CarCharge> charges = new List<CarCharge>();
            CarCharge charge = new CarCharge();
            charge.Id = 69;
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.CarCharges = DbContextMock.GetQueryableMockDbSet(charges);

            CarChargeRepository repo = new CarChargeRepository(myDbMoq.Object);

            Assert.True(repo.CreateCarCharge(charge));
        }

        [Fact]
        public void DeleteCarChargeTest()
        {
            List<CarCharge> charges = new List<CarCharge>();
            CarCharge charge = new CarCharge();
            charge.Id = 69;
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.CarCharges = DbContextMock.GetQueryableMockDbSet(charges);

            CarChargeRepository repo = new CarChargeRepository(myDbMoq.Object);

            Assert.True(repo.DeleteCarCharge(charge));
        }

        [Fact]
        public void DeleteCarChargesTest()
        {
            List<CarCharge> charges = new List<CarCharge>();
            CarCharge charge = new CarCharge();
            charge.Id = 69;
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.CarCharges = DbContextMock.GetQueryableMockDbSet(charges);

            CarChargeRepository repo = new CarChargeRepository(myDbMoq.Object);

            Assert.True(repo.DeleteCarCharges(charges));
        }

        [Fact]
        public void GetCarChargeTest()
        {
            List<CarCharge> charges = new List<CarCharge>();
            CarCharge charge = new CarCharge();
            charge.Id = 69;
            charges.Add(charge);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.CarCharges = DbContextMock.GetQueryableMockDbSet(charges);

            CarChargeRepository repo = new CarChargeRepository(myDbMoq.Object);

            Assert.Equal(charge, repo.GetCarCharge(69));
        }

        [Fact]
        public void GetCarChargesTest()
        {
            List<CarCharge> charges = new List<CarCharge>();
            CarCharge charge = new CarCharge();
            charge.Id = 69;
            charge.EndOfCharge = DateTime.Now; 
            charges.Add(charge);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.CarCharges = DbContextMock.GetQueryableMockDbSet(charges);

            CarChargeRepository repo = new CarChargeRepository(myDbMoq.Object);

            Assert.Equal(charges, repo.GetCarCharges());
        }

        [Fact]
        public void GetChargesOfACarTest()
        {
            List<CarCharge> charges = new List<CarCharge>();
            CarCharge charge = new CarCharge();
            charge.Id = 69;
            charge.Car = new Car();
            charge.Car.Id = 10;
            charges.Add(charge);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.CarCharges = DbContextMock.GetQueryableMockDbSet(charges);

            CarChargeRepository repo = new CarChargeRepository(myDbMoq.Object);

            Assert.Equal(charges, repo.GetChargesOfACar(10));
        }

        [Fact]
        public void SaveTest()
        {
            List<CarCharge> charges = new List<CarCharge>();
            CarCharge charge = new CarCharge();
            charge.Id = 69;
            charge.Car = new Car();
            charge.Car.Id = 10;
            charges.Add(charge);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.CarCharges = DbContextMock.GetQueryableMockDbSet(charges);

            CarChargeRepository repo = new CarChargeRepository(myDbMoq.Object);

            Assert.True(repo.Save());
        }

        [Fact]
        public void UpdateCarChargeTest()
        {
            List<CarCharge> charges = new List<CarCharge>();
            CarCharge charge = new CarCharge();
            charge.Id = 69;
            charge.Car = new Car();
            charge.Car.Id = 10;
            charges.Add(charge);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.CarCharges = DbContextMock.GetQueryableMockDbSet(charges);

            CarChargeRepository repo = new CarChargeRepository(myDbMoq.Object);

            Assert.True(repo.UpdateCarCharge(charge));
        }
    }
}