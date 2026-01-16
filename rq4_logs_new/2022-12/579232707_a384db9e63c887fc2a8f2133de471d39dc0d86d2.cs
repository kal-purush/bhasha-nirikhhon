

using Catcheap.API.Models.CarModels;
using Catcheap.API.Services.CarServices;
using Catcheap.Tests.CatcheapApi.MockRepo;

namespace Catcheap.Tests.CatcheapApi
{
    public class CarServiceTest
    {
        [Fact]
        public void DecreaseExpectedRangeTest()
        {
            Car car = new Car();
            car.ExpectedRange = 100;

            CarService service = new CarService(new MockCarRepository(), new CarChargeService(new MockCarChargeRepository(), new MockNordpoolPriceRepository()));

            service.DecreaseExpectedRange(car, 50);

            Assert.Equal(50,car.ExpectedRange);
        }

        [Fact]
        public void DecreaseBatteryLevelTest()
        {
            Car car = new Car();
            car.BatteryLevel = 100;
            car.Consumption = 10;
            car.BatteryCapacity = 100;

            CarService service = new CarService(new MockCarRepository(), new CarChargeService(new MockCarChargeRepository(), new MockNordpoolPriceRepository()));

            service.DecreaseBatteryLevel(car, 50);

            Assert.Equal(95, car.BatteryLevel);
        }

        [Fact]
        public void IncreaseBatteryLevelTest()
        {
            Car car = new Car();
            car.BatteryLevel = 10;
            car.BatteryCapacity = 100;

            CarService service = new CarService(new MockCarRepository(), new CarChargeService(new MockCarChargeRepository(), new MockNordpoolPriceRepository()));

            service.IncreaseBatteryLevel(car, 50);

            Assert.Equal(60, car.BatteryLevel);
        }

        [Fact]
        public void IncreaseMileageTest()
        {
            Car car = new Car();
            car.Mileage = 100;
            CarService service = new CarService(new MockCarRepository(), new CarChargeService(new MockCarChargeRepository(), new MockNordpoolPriceRepository()));

            service.IncreaseMileage(car, 100);

            Assert.Equal(200, car.Mileage);
        }

        [Fact]
        public void UpdateAfterJourneyTest()
        {
            Car car = new Car();
            car.ExpectedRange = 100;
            car.BatteryLevel = 100;
            car.Consumption = 10;
            car.BatteryCapacity = 100;
            car.Mileage = 100;
            CarService service = new CarService(new MockCarRepository(), new CarChargeService(new MockCarChargeRepository(), new MockNordpoolPriceRepository()));
            CarJourney carJourney = new CarJourney();
            carJourney.Distance = 50;

            service.UpdateAfterJourney(car, carJourney);

            Assert.Equal(50, car.ExpectedRange);
            Assert.Equal(95, car.BatteryLevel);
            Assert.Equal(150, car.Mileage);
        }

        [Fact]
        public void UpdateAfterChargeTest()
        {
            Car car = new Car();
            car.ExpectedRange = 100;
            car.BatteryLevel = 10;
            car.Consumption = 10;
            car.BatteryCapacity = 100;
            CarService service = new CarService(new MockCarRepository(), new CarChargeService(new MockCarChargeRepository(), new MockNordpoolPriceRepository()));
            CarCharge carCharge = new CarCharge();
            carCharge.StartOfCharge = new DateTime(2022, 12, 19, 12, 0, 0);
            carCharge.EndOfCharge = new DateTime(2022, 12, 19, 13, 0, 0);
            carCharge.ChargingPower = 10;


            service.UpdateAfterCharge(car, carCharge);

            Assert.Equal(200, car.ExpectedRange);
            Assert.Equal(20, car.BatteryLevel);
        }

        [Fact]
        public void CalculateExpectedRangeTest()
        {
            Car car = new Car();
            car.BatteryLevel = 100;
            car.Consumption = 100;
            car.BatteryCapacity = 100;
            CarService service = new CarService(new MockCarRepository(), new CarChargeService(new MockCarChargeRepository(), new MockNordpoolPriceRepository()));

            service.CalculateExpectedRange(car);

            Assert.Equal(100, car.ExpectedRange);
        }
    }
}