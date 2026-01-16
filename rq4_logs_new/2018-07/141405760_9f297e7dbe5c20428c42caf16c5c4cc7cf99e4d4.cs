using System;
using System.Collections.Generic;
using AutoMapper;
using BusinessLayer.Services;
using DataAccessLayer.Interfaces;
using DataAccessLayer.Models;
using FakeItEasy;
using NUnit.Framework;
using DTO = Shared.DTO;

namespace Airport.Tests.Services
{
    [TestFixture]
    public class AircraftServiceTests
    {
        private readonly IUnitOfWork _fakeUnitOfWork;
        private readonly IRepository<Aircraft> _fakeAircraftRepository;
        private readonly IRepository<AircraftType> _fakeAircraftTypeRepository;
        private readonly IMapper _fakeMapper;
        private AircraftService _aircraftService;
        private int _aircraftId;
        private Aircraft _plane1;
        private DTO.Aircraft _plane1DTO;

        public AircraftServiceTests()
        {
            _fakeUnitOfWork = A.Fake<IUnitOfWork>();
            _fakeAircraftRepository = A.Fake<IRepository<Aircraft>>();
            _fakeAircraftTypeRepository = A.Fake<IRepository<AircraftType>>();
            _fakeMapper = A.Fake<IMapper>();
        }

        // This method runs before each test.
        [SetUp]
        public void TestSetup()
        {
            _aircraftId = 1;
            _plane1 = new Aircraft()
            {
                Id = _aircraftId, AircraftName = "Strong",
                AircraftType = new AircraftType() { Id = 1, AircraftModel = "Tupolev Tu-134", SeatsNumber = 80, Carrying = 47000 },
                AircraftReleaseDate = new DateTime(2011, 6, 10), ExploitationTimeSpan = new DateTime(2021, 6, 10) - new DateTime(2011, 6, 10)
            };

            _plane1DTO = new DTO.Aircraft
            {
                Id = _aircraftId, AircraftName = "Strong",
                AircraftType = new DTO.AircraftType() { Id = 1, AircraftModel = "Tupolev Tu-134", SeatsNumber = 80, Carrying = 47000 },
                AircraftReleaseDate = new DateTime(2011, 6, 10), ExploitationTimeSpan = new DateTime(2021, 6, 10) - new DateTime(2011, 6, 10)
            };

            A.CallTo(() => _fakeMapper.Map<Aircraft, DTO.Aircraft>(_plane1)).Returns(_plane1DTO);
            A.CallTo(() => _fakeMapper.Map<DTO.Aircraft, Aircraft>(_plane1DTO)).Returns(_plane1);

            A.CallTo(() => _fakeUnitOfWork.AircraftRepository).Returns(_fakeAircraftRepository);
            A.CallTo(() => _fakeUnitOfWork.Set<AircraftType>()).Returns(_fakeAircraftTypeRepository);
            _aircraftService = new AircraftService(_fakeUnitOfWork, _fakeMapper);
        }

        // This method runs after each test.
        [TearDown]
        public void TestTearDown() {}

        [Test]
        public void ValidationForeignId_Should_ReturnTrue_When_TypeExists()
        {
            A.CallTo(() => _fakeUnitOfWork.Set<AircraftType>().Get(null)).Returns(new List<AircraftType> { _plane1.AircraftType });
            var result = _aircraftService.ValidationForeignId(_plane1DTO);
            Assert.IsTrue(result);
        }

        [Test]
        public void ValidationForeignId_Should_ReturnFalse_When_TypeDoesntExist()
        {
            A.CallTo(() => _fakeUnitOfWork.Set<AircraftType>().Get(null)).Returns(new List<AircraftType> ());
            var result = _aircraftService.ValidationForeignId(_plane1DTO);
            Assert.IsFalse(result);
        }

        [Test]
        public void IsExists_ShouldReturnAircraftDto_WhenAircraftExists()
        {
            A.CallTo(() => _fakeUnitOfWork.AircraftRepository.Get(_aircraftId)).Returns(new List<Aircraft> { _plane1 });
            var result = _aircraftService.IsExist(_aircraftId);
            Assert.AreEqual(_plane1DTO, result);
        }

        [Test]
        public void ConvertToModel_Should_ReturnModel_When_Called()
        {
            A.CallTo(() => _fakeMapper.Map<DTO.Aircraft, Aircraft>(_plane1DTO)).Returns(_plane1);
            var result = _aircraftService.ConvertToModel(_plane1DTO);
            Assert.AreEqual(_plane1, result);
        }
        
        [Test]
        public void GetAll_Should_ReturnListAircraftsDTO_When_Called()
        {
            A.CallTo(() => _fakeMapper.Map<List<Aircraft>, List<DTO.Aircraft>>(A<List<Aircraft>>.That.Contains(_plane1)))
                .Returns(new List<DTO.Aircraft> { _plane1DTO });
            A.CallTo(() => _fakeUnitOfWork.AircraftRepository.Get(null)).Returns(new List<Aircraft> { _plane1 });
            List<DTO.Aircraft> result = _aircraftService.GetAll();

            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(new List<DTO.Aircraft> { _plane1DTO }, result);
        }

        [Test]
        public void GetDetails_Should_ReturnAircraftDTO_When_Called()
        {
            A.CallTo(() => _fakeUnitOfWork.AircraftRepository.Get(_aircraftId)).Returns(new List<Aircraft> { _plane1 });
            var result = _aircraftService.GetDetails(_aircraftId);
            Assert.AreEqual(_plane1DTO, result);
        }

        [Test]
        public void Add_Should_CallRepositoryCreate_When_Called()
        {
            _aircraftService.Add(_plane1DTO);
            A.CallTo(() => _fakeUnitOfWork.AircraftRepository.Create(A<Aircraft>.That.IsInstanceOf(typeof(Aircraft)), null)).MustHaveHappenedOnceExactly();
        }

        [Test]
        public void Update_Should_CallRepositoryUpdate_When_Called()
        {
            _aircraftService.Update(_plane1DTO);
            A.CallTo(() => _fakeUnitOfWork.AircraftRepository.Update(A<Aircraft>.That.IsInstanceOf(typeof(Aircraft)), null)).MustHaveHappenedOnceExactly();
        }

        [Test]
        public void Remove_Should_CallRepositoryRemove_When_Called()
        {
            _aircraftService.Remove(_aircraftId);
            A.CallTo(() => _fakeUnitOfWork.AircraftRepository.Delete(_aircraftId)).MustHaveHappenedOnceExactly();
        }

        [Test]
        public void RemoveAll_Should_CallRepositoryRemoveAll_When_Called()
        {
            _aircraftService.RemoveAll();
            A.CallTo(() => _fakeUnitOfWork.AircraftRepository.Delete(null)).MustHaveHappenedOnceExactly();
        }
    }
}