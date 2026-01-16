using AutoMapper;
using Catcheap.API.Controllers.CarControl;
using Catcheap.API.DTO;
using Catcheap.API.Helper;
using Catcheap.Tests.CatcheapApi.MockRepo;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Catcheap.Tests.CatcheapApi.ControllerTests
{
    public class CarJourneyControllerTest
    {
        [Fact]
        public void GetCarJourneysTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            CarJourneyController controller = new CarJourneyController(new MockCarJourneyRepository(), new MockCarRepository(), mapper);

            var carJourney = (OkObjectResult)controller.GetCarJourneys();
            var rez = carJourney.Value;

            List<JourneyDTO> l = new List<JourneyDTO>();
            Assert.Equal(l.GetType(), rez.GetType());

        }

        [Fact]
        public void GetCarJourneyTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            CarJourneyController controller = new CarJourneyController(new MockCarJourneyRepository(), new MockCarRepository(), mapper);

            var carJourney = (OkObjectResult)controller.GetCarJourney(69);
            var rez = carJourney.Value;

            JourneyDTO journey = (JourneyDTO)rez;

            Assert.Equal(69, journey.Id);

        }

        [Fact]
        public void CreateCarJourneyTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            CarJourneyController controller = new CarJourneyController(new MockCarJourneyRepository(), new MockCarRepository(), mapper);

            JourneyDTO journeyDTO = new JourneyDTO();
            journeyDTO.Id = 69;
            var carJourney = (OkObjectResult)controller.CreateCarJourney(69, journeyDTO);

            Assert.Equal(200, carJourney.StatusCode);

        }

        [Fact]
        public void UpdateCarJourneyTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            CarJourneyController controller = new CarJourneyController(new MockCarJourneyRepository(), new MockCarRepository(), mapper);

            JourneyDTO journeyDTO = new JourneyDTO();
            journeyDTO.Id = 69;
            var carJourney = (NoContentResult)controller.UpdateCarJourney(69, journeyDTO);

            Assert.Equal(204, carJourney.StatusCode);

        }

        [Fact]
        public void DeleteCarJourneyTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            CarJourneyController controller = new CarJourneyController(new MockCarJourneyRepository(), new MockCarRepository(), mapper);

            var carJourney = (NoContentResult)controller.DeleteCarJourney(69);

            Assert.Equal(204, carJourney.StatusCode);

        }
    }
}