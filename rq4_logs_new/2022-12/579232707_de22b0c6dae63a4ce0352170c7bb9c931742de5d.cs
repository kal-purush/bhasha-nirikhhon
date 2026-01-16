using AutoMapper;
using Catcheap.API.Controllers.CarControl;
using Catcheap.API.DTO;
using Catcheap.API.Helper;
using Catcheap.API.Models.CarModels;
using Catcheap.API.Services.CarServices;
using Catcheap.Tests.CatcheapApi.MockRepo;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Catcheap.Tests.CatcheapApi.ControllerTests
{
    public class CarChargeControllerTest
    {

        [Fact]
        public void GetCarChargesTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            CarChargeController controller = new CarChargeController(new MockCarChargeRepository(), new MockCarRepository(),
                    new CarChargeService(new MockCarChargeRepository(), new MockNordpoolPriceRepository()), mapper);

            var carCharges = (OkObjectResult)controller.GetCarCharges();
            var rez = carCharges.Value;

            List<ChargeDTO> l = new List<ChargeDTO>();
            Assert.Equal(l.GetType(),rez.GetType());
            
        }

        [Fact]
        public void GetCarChargeTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            CarChargeController controller = new CarChargeController(new MockCarChargeRepository(), new MockCarRepository(),
                    new CarChargeService(new MockCarChargeRepository(), new MockNordpoolPriceRepository()), mapper);

            var carCharges = (OkObjectResult)controller.GetCarCharge(69);
            var rez = carCharges.Value;

            ChargeDTO charge = (ChargeDTO)rez;

            Assert.Equal(69,charge.Id);

        }

        [Fact]
        public void CreateCarChargeTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            CarChargeController controller = new CarChargeController(new MockCarChargeRepository(), new MockCarRepository(),
                    new CarChargeService(new MockCarChargeRepository(), new MockNordpoolPriceRepository()), mapper);

            var carCharges = (OkObjectResult)controller.CreateCarCharge(69, new ChargeDTO());

            Assert.Equal(200, carCharges.StatusCode);

        }

        [Fact]
        public void UpdateCarChargeTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            CarChargeController controller = new CarChargeController(new MockCarChargeRepository(), new MockCarRepository(),
                    new CarChargeService(new MockCarChargeRepository(), new MockNordpoolPriceRepository()), mapper);

            ChargeDTO charge = new ChargeDTO();
            charge.Id = 69;
            var carCharges = (NoContentResult)controller.UpdateCarCharge(69, charge);

            Assert.Equal(204, carCharges.StatusCode);

        }

        [Fact]
        public void DeleteCarChargeTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            CarChargeController controller = new CarChargeController(new MockCarChargeRepository(), new MockCarRepository(),
                    new CarChargeService(new MockCarChargeRepository(), new MockNordpoolPriceRepository()), mapper);

            var carCharges = (NoContentResult)controller.DeleteCarCharge(69);

            Assert.Equal(204, carCharges.StatusCode);

        }

        [Fact]
        public void CalculateChargePriceForCarTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            CarChargeController controller = new CarChargeController(new MockCarChargeRepository(), new MockCarRepository(),
                    new CarChargeService(new MockCarChargeRepository(), new MockNordpoolPriceRepository()), mapper);

            var carCharges = (NoContentResult)controller.CalculateChargePriceForCar(69);

            Assert.Equal(204, carCharges.StatusCode);

        }

    }
}