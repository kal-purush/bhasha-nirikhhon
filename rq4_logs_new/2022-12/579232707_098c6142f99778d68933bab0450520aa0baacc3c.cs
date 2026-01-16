using AutoMapper;
using Catcheap.API.Controllers.ScooterControl;
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
    public class ScooterJourneyControllerTest
    {

        [Fact]
        public void GetScooterJourneysTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            ScooterJourneyController controller = new ScooterJourneyController(new MockScooterJourneyRepository(), new MockScooterRepository(), mapper);

            var respons = (OkObjectResult)controller.GetScooterJourneys();
            var rez = respons.Value;

            List<JourneyDTO> l = new List<JourneyDTO>();
            Assert.Equal(l.GetType(), rez.GetType());

        }

        [Fact]
        public void GetScooterJourneyTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            ScooterJourneyController controller = new ScooterJourneyController(new MockScooterJourneyRepository(), new MockScooterRepository(), mapper);

            var respons = (OkObjectResult)controller.GetScooterJourney(69);
            var rez = respons.Value;

            JourneyDTO journey = (JourneyDTO)rez;

            Assert.Equal(69, journey.Id);

        }

        [Fact]
        public void CreateScooterJourneyTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            ScooterJourneyController controller = new ScooterJourneyController(new MockScooterJourneyRepository(), new MockScooterRepository(), mapper);

            JourneyDTO journeyDTO = new JourneyDTO();
            journeyDTO.Id = 69;
            var respons = (OkObjectResult)controller.CreateScooterJourney(69, journeyDTO);

            Assert.Equal(200, respons.StatusCode);

        }

        [Fact]
        public void UpdateScooterJourneyTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            ScooterJourneyController controller = new ScooterJourneyController(new MockScooterJourneyRepository(), new MockScooterRepository(), mapper);

            JourneyDTO journeyDTO = new JourneyDTO();
            journeyDTO.Id = 69;
            var respons = (NoContentResult)controller.UpdateScooterJourney(69, journeyDTO);

            Assert.Equal(204, respons.StatusCode);

        }

        [Fact]
        public void DeleteScooterJourneyTest()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new MappingProfiles());
            });
            IMapper mapper = mappingConfig.CreateMapper();

            ScooterJourneyController controller = new ScooterJourneyController(new MockScooterJourneyRepository(), new MockScooterRepository(), mapper);

            var respons = (NoContentResult)controller.DeleteScooterJourney(69);

            Assert.Equal(204, respons.StatusCode);

        }
    }
}