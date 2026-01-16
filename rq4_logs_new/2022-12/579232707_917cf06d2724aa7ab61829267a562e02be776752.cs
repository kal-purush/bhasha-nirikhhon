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
    public class ScooterControllerTest
    {
        [Fact]
        public void GetScootersTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            ScooterController controller = new ScooterController(new MockScooterRepository(), new MockScooterJourneyRepository(), new MockScooterChargeRepository(),
                    mapper, new ScooterService(new MockScooterRepository(), new ScooterChargeService(new MockScooterChargeRepository(), new MockNordpoolPriceRepository())));

            var respons = (OkObjectResult)controller.GetScooters();
            var rez = respons.Value;

            List<ScooterDTO> l = new List<ScooterDTO>();
            Assert.Equal(l.GetType(), rez.GetType());

        }

        [Fact]
        public void GetScooterTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            ScooterController controller = new ScooterController(new MockScooterRepository(), new MockScooterJourneyRepository(), new MockScooterChargeRepository(),
                    mapper, new ScooterService(new MockScooterRepository(), new ScooterChargeService(new MockScooterChargeRepository(), new MockNordpoolPriceRepository())));

            var respons = (OkObjectResult)controller.GetScooter(69);
            ScooterDTO rez = (ScooterDTO)respons.Value;

            Assert.Equal(69, rez.Id);

        }

        [Fact]
        public void GetChargesOfAScooterTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            ScooterController controller = new ScooterController(new MockScooterRepository(), new MockScooterJourneyRepository(), new MockScooterChargeRepository(),
                    mapper, new ScooterService(new MockScooterRepository(), new ScooterChargeService(new MockScooterChargeRepository(), new MockNordpoolPriceRepository())));

            var respons = (OkObjectResult)controller.GetChargesOfAScooter(69);
            List<ChargeDTO> rez = (List<ChargeDTO>)respons.Value;

            Assert.Equal(10, rez[0].ChargingPrice);

        }

        [Fact]
        public void GetJourneysOfAScooterTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            ScooterController controller = new ScooterController(new MockScooterRepository(), new MockScooterJourneyRepository(), new MockScooterChargeRepository(),
                    mapper, new ScooterService(new MockScooterRepository(), new ScooterChargeService(new MockScooterChargeRepository(), new MockNordpoolPriceRepository())));

            var respons = (OkObjectResult)controller.GetJourneysOfAScooter(69);
            List<JourneyDTO> rez = (List<JourneyDTO>)respons.Value;

            Assert.Equal(100, rez[0].Distance);

        }

        [Fact]
        public void CreateScooterTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            ScooterController controller = new ScooterController(new MockScooterRepository(), new MockScooterJourneyRepository(), new MockScooterChargeRepository(),
                    mapper, new ScooterService(new MockScooterRepository(), new ScooterChargeService(new MockScooterChargeRepository(), new MockNordpoolPriceRepository())));

            var respons = (OkObjectResult)controller.GetJourneysOfAScooter(69);

            Assert.Equal(200, respons.StatusCode);

        }

        [Fact]
        public void UpdateScooterTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            ScooterController controller = new ScooterController(new MockScooterRepository(), new MockScooterJourneyRepository(), new MockScooterChargeRepository(),
                    mapper, new ScooterService(new MockScooterRepository(), new ScooterChargeService(new MockScooterChargeRepository(), new MockNordpoolPriceRepository())));

            ScooterDTO scooter = new ScooterDTO();
            scooter.Id = 69;
            var respons = (NoContentResult)controller.UpdateScooter(69, scooter);

            Assert.Equal(204, respons.StatusCode);

        }

        [Fact]
        public void DeleteScooterTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            ScooterController controller = new ScooterController(new MockScooterRepository(), new MockScooterJourneyRepository(), new MockScooterChargeRepository(),
                    mapper, new ScooterService(new MockScooterRepository(), new ScooterChargeService(new MockScooterChargeRepository(), new MockNordpoolPriceRepository())));

            ScooterDTO scooter = new ScooterDTO();
            scooter.Id = 69;
            var respons = (NoContentResult)controller.DeleteScooter(69);

            Assert.Equal(204, respons.StatusCode);

        }

        [Fact]
        public void UpdateScooterAfterJourneyTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            ScooterController controller = new ScooterController(new MockScooterRepository(), new MockScooterJourneyRepository(), new MockScooterChargeRepository(),
                    mapper, new ScooterService(new MockScooterRepository(), new ScooterChargeService(new MockScooterChargeRepository(), new MockNordpoolPriceRepository())));

            ScooterDTO scooter = new ScooterDTO();
            scooter.Id = 69;
            var respons = (NoContentResult)controller.UpdateScooterAfterJourney(69, 10);

            Assert.Equal(204, respons.StatusCode);

        }

        [Fact]
        public void UpdateScooterAfterChargeTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            ScooterController controller = new ScooterController(new MockScooterRepository(), new MockScooterJourneyRepository(), new MockScooterChargeRepository(),
                    mapper, new ScooterService(new MockScooterRepository(), new ScooterChargeService(new MockScooterChargeRepository(), new MockNordpoolPriceRepository())));

            ScooterDTO scooter = new ScooterDTO();
            scooter.Id = 69;
            var respons = (NoContentResult)controller.UpdateScooterAfterCharge(69, 10);

            Assert.Equal(204, respons.StatusCode);

        }

        [Fact]
        public void CalculateExpectedRangeForScooterTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            ScooterController controller = new ScooterController(new MockScooterRepository(), new MockScooterJourneyRepository(), new MockScooterChargeRepository(),
                    mapper, new ScooterService(new MockScooterRepository(), new ScooterChargeService(new MockScooterChargeRepository(), new MockNordpoolPriceRepository())));

            var respons = (NoContentResult)controller.CalculateExpectedRangeForScooter(69);

            Assert.Equal(204, respons.StatusCode);

        }
    }
}