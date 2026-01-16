using AutoMapper;
using Catcheap.API.Controllers.ScooterControl;
using Catcheap.API.DTO;
using Catcheap.API.Helper;
using Catcheap.API.Services.ScooterServices;
using Catcheap.Tests.CatcheapApi.MockRepo;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Catcheap.Tests.CatcheapApi.ControllerTests
{
    public class ScooterChargeControllerTest
    {
        [Fact]
        public void GetScooterChargesTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            ScooterChargeController controller = new ScooterChargeController(new MockScooterChargeRepository(), new MockScooterRepository(),
                    new ScooterChargeService(new MockScooterChargeRepository(), new MockNordpoolPriceRepository()), mapper);

            var respons = (OkObjectResult)controller.GetScooterCharges();
            var rez = respons.Value;

            List<ChargeDTO> l = new List<ChargeDTO>();
            Assert.Equal(l.GetType(), rez.GetType());

        }

        [Fact]
        public void GetScooterChargeTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            ScooterChargeController controller = new ScooterChargeController(new MockScooterChargeRepository(), new MockScooterRepository(),
                    new ScooterChargeService(new MockScooterChargeRepository(), new MockNordpoolPriceRepository()), mapper);

            var respons = (OkObjectResult)controller.GetScooterCharge(69);
            var rez = respons.Value;

            ChargeDTO charge = (ChargeDTO)rez;

            Assert.Equal(69, charge.Id);

        }

        [Fact]
        public void CreateScooterChargeTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            ScooterChargeController controller = new ScooterChargeController(new MockScooterChargeRepository(), new MockScooterRepository(),
                    new ScooterChargeService(new MockScooterChargeRepository(), new MockNordpoolPriceRepository()), mapper);

            var respons = (OkObjectResult)controller.CreateScooterCharge(69, new ChargeDTO());

            Assert.Equal(200, respons.StatusCode);

        }

        [Fact]
        public void UpdateScooterChargeTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            ScooterChargeController controller = new ScooterChargeController(new MockScooterChargeRepository(), new MockScooterRepository(),
                    new ScooterChargeService(new MockScooterChargeRepository(), new MockNordpoolPriceRepository()), mapper);

            ChargeDTO charge = new ChargeDTO();
            charge.Id = 69;
            var respons = (NoContentResult)controller.UpdateScooterCharge(69, charge);

            Assert.Equal(204, respons.StatusCode);

        }

        [Fact]
        public void DeleteScooterChargeTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            ScooterChargeController controller = new ScooterChargeController(new MockScooterChargeRepository(), new MockScooterRepository(),
                    new ScooterChargeService(new MockScooterChargeRepository(), new MockNordpoolPriceRepository()), mapper);

            var respons = (NoContentResult)controller.DeleteScooterCharge(69);

            Assert.Equal(204, respons.StatusCode);

        }

        [Fact]
        public void CalculateChargePriceForScooterTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            ScooterChargeController controller = new ScooterChargeController(new MockScooterChargeRepository(), new MockScooterRepository(),
                    new ScooterChargeService(new MockScooterChargeRepository(), new MockNordpoolPriceRepository()), mapper);

            var respons = (NoContentResult)controller.CalculateChargePriceForScooter(69);

            Assert.Equal(204, respons.StatusCode);

        }
    }
}