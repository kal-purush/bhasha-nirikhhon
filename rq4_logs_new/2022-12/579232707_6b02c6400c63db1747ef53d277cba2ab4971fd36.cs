using Catcheap.API.Data;
using Catcheap.API.Models.CarModels;
using Catcheap.API.Repositories.CarRepo;
using Microsoft.EntityFrameworkCore;
using Moq;

namespace Catcheap.Tests.CatcheapApi.RepoTests
{
    public class CarRepositoryTest
    {
        [Fact]
        public void GetCarTest()
        {
            List<Car> cars = new List<Car>();
            Car car = new Car();
            car.Id = 69;
            cars.Add(car);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.Cars = DbContextMock.GetQueryableMockDbSet(cars);

            CarRepository repo = new CarRepository(myDbMoq.Object);

            Assert.Equal(car,repo.GetCar(69));
        }

        [Fact]
        public void CarExistsTest()
        {
            List<Car> cars = new List<Car>();
            Car car = new Car();
            car.Id = 69;
            cars.Add(car);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.Cars = DbContextMock.GetQueryableMockDbSet(cars);

            CarRepository repo = new CarRepository(myDbMoq.Object);

            Assert.True(repo.CarExists(69));
        }

        [Fact]
        public void GetCarsTest()
        {
            List<Car> cars = new List<Car>();
            Car car = new Car();
            car.Id = 69;
            cars.Add(car);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.Cars = DbContextMock.GetQueryableMockDbSet(cars);

            CarRepository repo = new CarRepository(myDbMoq.Object);

            Assert.Equal(cars,repo.GetCars());
        }

        [Fact]
        public void CreateCarTest()
        {
            List<Car> cars = new List<Car>();
            Car car = new Car();
            car.Id = 69;
            cars.Add(car);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.Cars = DbContextMock.GetQueryableMockDbSet(cars);

            CarRepository repo = new CarRepository(myDbMoq.Object);

            Assert.True(repo.CreateCar(car));
        }

        [Fact]
        public void SaveTest()
        {
            List<Car> cars = new List<Car>();
            Car car = new Car();
            car.Id = 69;
            cars.Add(car);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.Cars = DbContextMock.GetQueryableMockDbSet(cars);

            CarRepository repo = new CarRepository(myDbMoq.Object);

            Assert.True(repo.Save());
        }

        [Fact]
        public void UpdateCarTest()
        {
            List<Car> cars = new List<Car>();
            Car car = new Car();
            car.Id = 69;
            cars.Add(car);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.Cars = DbContextMock.GetQueryableMockDbSet(cars);

            CarRepository repo = new CarRepository(myDbMoq.Object);

            Assert.True(repo.UpdateCar(car));
        }

        [Fact]
        public void DeleteCarTest()
        {
            List<Car> cars = new List<Car>();
            Car car = new Car();
            car.Id = 69;
            cars.Add(car);
            DbContextOptions<DataContext> options = new DbContextOptions<DataContext>();
            var myDbMoq = new Mock<DataContext>(options);
            myDbMoq.SetupAllProperties();
            myDbMoq.Setup(p => p.SaveChanges()).Returns(1);
            myDbMoq.Object.Cars = DbContextMock.GetQueryableMockDbSet(cars);

            CarRepository repo = new CarRepository(myDbMoq.Object);

            Assert.True(repo.DeleteCar(car));
        }
    }
}