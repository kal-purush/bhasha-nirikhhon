using Moq;
using OffiRent.API.Domain.Models;
using OffiRent.API.Domain.Repositories;
using System;
using NUnit.Framework;
using FluentAssertions;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using OffiRent.API.Services;
using OffiRent.API.Domain.Services.Communications;

namespace OffiRent.API.Test
{
    class OfficeServiceTest
    {

        [Test]
        public async Task GetByIdAsynWhenExistingIdReturnsDetailsOffice()
        {
            //Arrange
            var mockOfficeRepository = GetDefaultIOfficeRepositoryInstance();
            var officeId = 70;
            Office falseoffice = new Office { Id = 70, Address = "calle Jerusalen", Floor = 2, Capacity = "4 personas", AllowResource = true, Punctuation = "85 puntos", Description = "Oficina espaciosa con gran comodidad", Price = 100, Status = true, AccountId = 300, DistrictId = 80 };

            mockOfficeRepository.Setup(o => o.FindById(officeId))
                .ReturnsAsync(falseoffice);

            var mockUnitOfWork = GetDefaultIUnitOfWorkInstance();

            var service = new OfficeService(mockOfficeRepository.Object, mockUnitOfWork.Object);

            //Act
            var offices2 = (OfficeResponse)await service.GetByIdAsync(officeId);


            //Act
            offices2.Should().NotBeNull();

        }

        [Test]
        public async Task GetByIdAsyncWhenInvalidIdRetunrsOfficeNotFoundResponse() 
        {
            //Arrange
            var mockOfficeRepository = GetDefaultIOfficeRepositoryInstance();
            var officeId = 1;                                                     
            mockOfficeRepository.Setup(r => r.FindById(officeId))              
                .Returns(Task.FromResult<Office>(null));                        

            var mockUnitOfWork = GetDefaultIUnitOfWorkInstance();

            var service = new OfficeService(
                mockOfficeRepository.Object,
                mockUnitOfWork.Object);

            //Act                                                                   
            OfficeResponse response = await service.GetByIdAsync(officeId);      
            var message = response.Message;

            //Assert
            message.Should().Be("Office not found");                      
        }

        [Test]
        public async Task GetByDistrictIdAsynWhenExistingIdReturnsCollectionNotEmpty()
        {
            //Arrange
            var mockOfficeRepository = GetDefaultIOfficeRepositoryInstance();
            var districtId = 80;
            List<Office> offices= new List<Office>();
            offices.Add(new Office { Id = 70, Address = "calle Jerusalen", Floor = 2, Capacity = "4 personas", AllowResource = true, Punctuation = "85 puntos", Description = "Oficina espaciosa con gran comodidad", Price = 100, Status = true, AccountId = 300, DistrictId = 80 });
            offices.Add(new Office { Id = 71, Address = "calle Jazmines", Floor = 1, Capacity = "3 personas", AllowResource = true, Punctuation = "99 puntos", Description = "Oficina grande", Price = 80, Status = true, AccountId = 300, DistrictId = 80 });

            mockOfficeRepository.Setup(o => o.ListByDistrictIdAsync(districtId))
                .ReturnsAsync(offices);

            var mockUnitOfWork = GetDefaultIUnitOfWorkInstance();

            var service = new OfficeService(mockOfficeRepository.Object, mockUnitOfWork.Object);

            //Act
            List<Office> offices2 = (List<Office>)await service.ListByDistrictIdAsync(districtId);
 

            //Act
            offices2.Should().NotBeEmpty();

        }


        [Test]
        public async Task GetByDistrictIdAsynWhenInValidIdReturnsCollectionEmpty()
        {
            // Arrange
            var mockOfficeRepository = GetDefaultIOfficeRepositoryInstance();
            var districtInvalidId = 12;
            mockOfficeRepository.Setup(r => r.ListByDistrictIdAsync(districtInvalidId))
                .ReturnsAsync(new List<Office>());

            var mockUnitOfWork = GetDefaultIUnitOfWorkInstance();
            var service = new OfficeService(mockOfficeRepository.Object, mockUnitOfWork.Object);

            // Act
            List<Office> offices = (List<Office>)await service.ListByDistrictIdAsync(districtInvalidId);
            var officesCount = offices.Count;

            // Assert
            officesCount.Should().Equals(0);
        }



        [Test]
        public async Task GetByPriceAsyncWhenPriceIswithinRankOfExistingPricesRetunrsCollectionNotEmpty() 
        {
            //Arrange
            var mockOfficeRepository = GetDefaultIOfficeRepositoryInstance();
            var price = 120;

            List<Office> falseoffices = new List<Office>();
            falseoffices.Add(new Office { Id = 70, Address = "calle Jerusalen", Floor = 2, Capacity = "4 personas", AllowResource = true, Punctuation = "85 puntos", Description = "Oficina espaciosa con gran comodidad", Price = 100, Status = true, AccountId = 300, DistrictId = 80 });
            falseoffices.Add(new Office { Id = 71, Address = "calle Jazmines", Floor = 1, Capacity = "3 personas", AllowResource = true, Punctuation = "99 puntos", Description = "Oficina grande", Price = 80, Status = true, AccountId = 300, DistrictId = 80 });

            mockOfficeRepository.Setup(o => o.ListByPriceEqualOrLowerThanAsync(price))
                .ReturnsAsync(falseoffices);

            var mockUnitOfWork = GetDefaultIUnitOfWorkInstance();

            var service = new OfficeService(mockOfficeRepository.Object, mockUnitOfWork.Object);

            //Act
            List<Office> offices = (List<Office>)await service.ListByPriceEqualOrLowerThanAsync(price);


            //Act
            offices.Should().NotBeEmpty();
        }

        [Test]
        public async Task GetByPriceIdAsynWhenInValidIdReturnsCollectionEmpty()
        {
            // Arrange
            var mockOfficeRepository = GetDefaultIOfficeRepositoryInstance();
            var invalidprice = 10;
            mockOfficeRepository.Setup(r => r.ListByPriceEqualOrLowerThanAsync(invalidprice))
                .ReturnsAsync(new List<Office>());

            var mockUnitOfWork = GetDefaultIUnitOfWorkInstance();
            var service = new OfficeService(mockOfficeRepository.Object, mockUnitOfWork.Object);

            // Act
            List<Office> offices = (List<Office>)await service.ListByPriceEqualOrLowerThanAsync(invalidprice);
            var officesCount = offices.Count;

            // Assert
            officesCount.Should().Equals(0);
        }


        private Mock<IOfficeRepository> GetDefaultIOfficeRepositoryInstance()
        {
            return new Mock<IOfficeRepository>();
        }


        private Mock<IUnitOfWork> GetDefaultIUnitOfWorkInstance()
        {
            return new Mock<IUnitOfWork>();
        }


    }
}