using AutoMapper;
using Catcheap.API.Controllers.CarControl;
using Catcheap.API.DTO;
using Catcheap.API.Helper;
using Catcheap.API.Services.CarServices;
using Catcheap.Tests.CatcheapApi.MockRepo;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Catcheap.Tests.CatcheapApi.ControllerTests
{
    public class CarControllerTest
    {
        [Fact]
        public void GetCarsTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            CarController controller = new CarController(new MockCarRepository(), new MockCarJourneyRepository(), new MockCarChargeRepository(),
                    mapper, new CarService(new MockCarRepository(), new CarChargeService(new MockCarChargeRepository(), new MockNordpoolPriceRepository())));

            var carCharges = (OkObjectResult)controller.GetCars();
            var rez = carCharges.Value;

            List<CarDTO> l = new List<CarDTO>();
            Assert.Equal(l.GetType(), rez.GetType());

        }

        [Fact]
        public void GetCarTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            CarController controller = new CarController(new MockCarRepository(), new MockCarJourneyRepository(), new MockCarChargeRepository(),
                    mapper, new CarService(new MockCarRepository(), new CarChargeService(new MockCarChargeRepository(), new MockNordpoolPriceRepository())));

            var carCharges = (OkObjectResult)controller.GetCar(69);
            CarDTO rez = (CarDTO)carCharges.Value;

            Assert.Equal(69, rez.Id);

        }

        [Fact]
        public void GetChargesOfACarTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            CarController controller = new CarController(new MockCarRepository(), new MockCarJourneyRepository(), new MockCarChargeRepository(),
                    mapper, new CarService(new MockCarRepository(), new CarChargeService(new MockCarChargeRepository(), new MockNordpoolPriceRepository())));

            var carCharges = (OkObjectResult)controller.GetChargesOfACar(69);
            List<ChargeDTO> rez = (List<ChargeDTO>)carCharges.Value;

            Assert.Equal(10, rez[0].ChargingPrice);

        }

        [Fact]
        public void GetJourneysOfACarTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            CarController controller = new CarController(new MockCarRepository(), new MockCarJourneyRepository(), new MockCarChargeRepository(),
                    mapper, new CarService(new MockCarRepository(), new CarChargeService(new MockCarChargeRepository(), new MockNordpoolPriceRepository())));

            var carCharges = (OkObjectResult)controller.GetJourneysOfACar(69);
            List<JourneyDTO> rez = (List<JourneyDTO>)carCharges.Value;

            Assert.Equal(100, rez[0].Distance);

        }

        [Fact]
        public void CreateCarTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            CarController controller = new CarController(new MockCarRepository(), new MockCarJourneyRepository(), new MockCarChargeRepository(),
                    mapper, new CarService(new MockCarRepository(), new CarChargeService(new MockCarChargeRepository(), new MockNordpoolPriceRepository())));

            var carCharges = (OkObjectResult)controller.GetJourneysOfACar(69);

            Assert.Equal(200, carCharges.StatusCode);

        }

        [Fact]
        public void UpdateCarTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            CarController controller = new CarController(new MockCarRepository(), new MockCarJourneyRepository(), new MockCarChargeRepository(),
                    mapper, new CarService(new MockCarRepository(), new CarChargeService(new MockCarChargeRepository(), new MockNordpoolPriceRepository())));

            CarDTO car = new CarDTO();
            car.Id = 69;
            var carCharges = (NoContentResult)controller.UpdateCar(69, car);

            Assert.Equal(204, carCharges.StatusCode);

        }

        [Fact]
        public void DeleteCarTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            CarController controller = new CarController(new MockCarRepository(), new MockCarJourneyRepository(), new MockCarChargeRepository(),
                    mapper, new CarService(new MockCarRepository(), new CarChargeService(new MockCarChargeRepository(), new MockNordpoolPriceRepository())));

            CarDTO car = new CarDTO();
            car.Id = 69;
            var carCharges = (NoContentResult)controller.DeleteCar(69);

            Assert.Equal(204, carCharges.StatusCode);

        }

        [Fact]
        public void UpdateCarAfterJourneyTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            CarController controller = new CarController(new MockCarRepository(), new MockCarJourneyRepository(), new MockCarChargeRepository(),
                    mapper, new CarService(new MockCarRepository(), new CarChargeService(new MockCarChargeRepository(), new MockNordpoolPriceRepository())));

            CarDTO car = new CarDTO();
            car.Id = 69;
            var carCharges = (NoContentResult)controller.UpdateCarAfterJourney(69, 10);

            Assert.Equal(204, carCharges.StatusCode);

        }

        [Fact]
        public void UpdateCarAfterChargeTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            CarController controller = new CarController(new MockCarRepository(), new MockCarJourneyRepository(), new MockCarChargeRepository(),
                    mapper, new CarService(new MockCarRepository(), new CarChargeService(new MockCarChargeRepository(), new MockNordpoolPriceRepository())));

            CarDTO car = new CarDTO();
            car.Id = 69;
            var carCharges = (NoContentResult)controller.UpdateCarAfterCharge(69, 10);

            Assert.Equal(204, carCharges.StatusCode);

        }

        [Fact]
        public void CalculateExpectedRangeForCarTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            CarController controller = new CarController(new MockCarRepository(), new MockCarJourneyRepository(), new MockCarChargeRepository(),
                    mapper, new CarService(new MockCarRepository(), new CarChargeService(new MockCarChargeRepository(), new MockNordpoolPriceRepository())));

            var carCharges = (NoContentResult)controller.CalculateExpectedRangeForCar(69);

            Assert.Equal(204, carCharges.StatusCode);

        }
    }
}