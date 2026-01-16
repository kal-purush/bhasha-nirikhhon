using AirportTests.Fakes;
using AirportTests.Modules;
using AutoMapper;
using Business_Layer.DTOValidation;
using Business_Layer.MyMapperConfiguration;
using Business_Layer.Services;
using Data_Access_Layer.Models;
using FluentValidation;
using NUnit.Framework;
using Shared.DTOs;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace AirportTests
{
    [TestFixture]
    public class CreateUpdateTests
    {
        AirportService _service;
        IMapper _mapper;
        public CreateUpdateTests()
        {
            _mapper = MyMapperConfiguration.GetConfiguration().CreateMapper();
            var unitOfWork = new FakeUnitOfWork();
            _service = new AirportService(unitOfWork);
        }

        [Test]
        public async Task ValidationMappingPilot_when_validate_pilot_OK_then_map()
        {
            var pilotDTOValidator = new PilotDTOValidator();
            PilotDTO correct = new PilotDTO() 
            {
                Id = 1,
                Surname = "Surname",
                Experience = 3,
                Name = "Name"
            };

            PilotDTO incorrect = new PilotDTO()
            {
                Id = 2
            };

            bool correctRes =  pilotDTOValidator.Validate(correct).IsValid;

            Assert.True(correctRes);
            var mapped = _mapper.Map<PilotDTO, Pilot>(correct);

            if (correctRes)
            {
               await _service.Post(mapped);
            }

            bool incorrectRes = pilotDTOValidator.Validate(incorrect).IsValid;

            Assert.False(incorrectRes);
            var mappedIncorrect = _mapper.Map<PilotDTO, Pilot>(incorrect);
            
            if(incorrectRes)
            {
                await _service.Post(mapped);
            }
        }

        [Test]
        public async Task ValidationMappingCrew_when_validate_pilot_OK_then_map()
        {
            var validator = new CrewDTOValidator();
            CrewDTO correct = new CrewDTO()
            {
                Id = 1,
                Stewardesses = new List<StewardessDTO>
                {
                    new StewardessDTO() { Id = 1, CrewId = 1 }
                },
                Pilot = new PilotDTO() { Id = 5, Experience = 3, Surname = "sur"}

            };

            CrewDTO incorrect = new CrewDTO()
            {
                Id = 2
            };

            bool correctRes = validator.Validate(correct).IsValid;

            Assert.True(correctRes);
            var mapped = _mapper.Map<CrewDTO, Crew>(correct);

            if (correctRes)
            {
                await _service.Post(mapped);
            }

            bool incorrectRes = validator.Validate(incorrect).IsValid;

            Assert.False(incorrectRes);
            var mappedIncorrect = _mapper.Map<CrewDTO, Crew>(incorrect);

            if (incorrectRes)
            {
                await _service.Post(mapped);
            }
        }

        [Test]
        public async Task ValidationMappingDeparture_when_validate_pilot_OK_then_map()
        {
            var validator = new DepartureDTOValidator();
            DepartureDTO correct = new DepartureDTO()
            {
                Id = 1,
                Crew = new CrewDTO(),
                Flight = new FlightDTO(),
                Plane = new PlaneDTO(),
                TimeOfDeparture = new System.DateTime(2018, 10, 10)
            };

            DepartureDTO incorrect = new DepartureDTO()
            {
                Id = 2,
                Crew = new CrewDTO(),
                Plane = new PlaneDTO()
            };

            bool correctRes = validator.Validate(correct).IsValid;

            Assert.True(correctRes);
            var mapped = _mapper.Map<DepartureDTO, Departure>(correct);

            if (correctRes)
            {
                await _service.Post(mapped);
            }

            bool incorrectRes = validator.Validate(incorrect).IsValid;

            Assert.False(incorrectRes);
            var mappedIncorrect = _mapper.Map<DepartureDTO, Departure>(incorrect);

            if (incorrectRes)
            {
                await _service.Post(mapped);
            }
        }

        [Test]
        public async Task ValidationMappingFlight_when_validate_pilot_OK_then_map()
        {
            var validator = new FlightDTOValidator();
            FlightDTO correct = new FlightDTO()
            {
                Number = 1,
                Destination = "dest",
                DepartureFrom = "depFrom"

            };

            FlightDTO incorrect = new FlightDTO()
            {
                Number = 2,
                TimeOfDeparture = new System.DateTime(2018, 10, 10),
                Destination = "dest"
            };

            bool correctRes = validator.Validate(correct).IsValid;

            Assert.True(correctRes);
            var mapped = _mapper.Map<FlightDTO, Flight>(correct);

            if (correctRes)
            {
                await _service.Post(mapped);
            }

            bool incorrectRes = validator.Validate(incorrect).IsValid;

            Assert.False(incorrectRes);
            var mappedIncorrect = _mapper.Map<FlightDTO, Flight>(incorrect);

            if (incorrectRes)
            {
                await _service.Post(mapped);
            }
        }

        [Test]
        public async Task ValidationMappingPlane_when_validate_pilot_OK_then_map()
        {
            var validator = new PlaneDTOValidator();
            PlaneDTO correct = new PlaneDTO()
            {
                Id = 1,
                PlaneType = new PlaneTypeDTO() { Id = 5, Model = "model" }
            };

            PlaneDTO incorrect = new PlaneDTO()
            {
                Id = 2,
                ReleaseDate = new System.DateTime(2012, 10, 10)
            };

            bool correctRes = validator.Validate(correct).IsValid;

            Assert.True(correctRes);
            var mapped = _mapper.Map<PlaneDTO, Plane>(correct);

            if (correctRes)
            {
                await _service.Post(mapped);
            }

            bool incorrectRes = validator.Validate(incorrect).IsValid;

            Assert.False(incorrectRes);
            var mappedIncorrect = _mapper.Map<PlaneDTO, Plane>(incorrect);

            if (incorrectRes)
            {
               await _service.Post(mapped);
            }
        }

        [Test]
        public async Task ValidationMappingPlaneType_when_validate_pilot_OK_then_map()
        {
            var validator = new PlaneTypeDTOValidator();
            PlaneTypeDTO correct = new PlaneTypeDTO()
            {
                Id = 1,
                Model = "Model"
            };

            PlaneTypeDTO incorrect = new PlaneTypeDTO()
            {
                Id = 2,
                NumberOfSeats = 25
            };

            bool correctRes = validator.Validate(correct).IsValid;

            Assert.True(correctRes);
            var mapped = _mapper.Map<PlaneTypeDTO, PlaneType>(correct);

            if (correctRes)
            {
                await _service.Post(mapped);
            }

            bool incorrectRes = validator.Validate(incorrect).IsValid;

            Assert.False(incorrectRes);
            var mappedIncorrect = _mapper.Map<PlaneTypeDTO, PlaneType>(incorrect);

            if (incorrectRes)
            {
                await _service.Post(mapped);
            }
        }

        [Test]
        public async Task ValidationMappingStewardess_when_validate_pilot_OK_then_map()
        {
            var validator = new StewardessDTOValidator();
            StewardessDTO correct = new StewardessDTO()
            {
                Id = 1,
                Surname = "sur"
            };

            StewardessDTO incorrect = new StewardessDTO()
            {
                Id = 2,
                Name = "name"
            };

            bool correctRes = validator.Validate(correct).IsValid;

            Assert.True(correctRes);
            var mapped = _mapper.Map<StewardessDTO, Stewardess>(correct);

            if (correctRes)
            {
                await _service.Post(mapped);
            }

            bool incorrectRes = validator.Validate(incorrect).IsValid;

            Assert.False(incorrectRes);
            var mappedIncorrect = _mapper.Map<StewardessDTO, Stewardess>(incorrect);

            if (incorrectRes)
            {
                await _service.Post(mapped);
            }
        }

        [Test]
        public async Task ValidationMappingTicket_when_validate_pilot_OK_then_map()
        {
            var validator = new TicketDTOValidator();
            TicketDTO correct = new TicketDTO()
            {
                Id = 1,
                FlightNumber = 5
            };

            TicketDTO incorrect = new TicketDTO()
            {
                Id = 2,
                FlightNumber = 0,
                Price = 55
            };

            bool correctRes = validator.Validate(correct).IsValid;

            Assert.True(correctRes);
            var mapped = _mapper.Map<TicketDTO, Ticket>(correct);

            if (correctRes)
            {
                await _service.Post(mapped);
            }

            bool incorrectRes = validator.Validate(incorrect).IsValid;

            Assert.False(incorrectRes);
            var mappedIncorrect = _mapper.Map<TicketDTO, Ticket>(incorrect);

            if (incorrectRes)
            {
                await _service.Post(mapped);
            }
        }
    }
}