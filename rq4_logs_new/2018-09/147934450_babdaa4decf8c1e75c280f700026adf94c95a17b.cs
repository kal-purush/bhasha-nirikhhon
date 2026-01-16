using System;
using System.Collections.Generic;
using Xunit;
using Moq;
using Tapir.Services;
using Tapir.Core;
using Tapir.Models;

namespace Tapir.UnitTests.Services
{
    public class CompanyServicesUnitTests
    {
        private readonly CompanyService companyService;
        private List<Company> companies;

        public CompanyServicesUnitTests()
        {
            companies = new List<Company>
            {
                new Company(){ID=1, FullName="Company1 Ltd", ShortName="Ltd1"},
                new Company(){ID=2, FullName="Company2 Ltd", ShortName="Ltd2"},
                new Company(){ID=3, FullName="Company3 Ltd", ShortName="Ltd3"}
            };
            var mockRepository = new Mock<ICompaniesRepository>();
            mockRepository.Setup(x => x.GetAll()).Returns(companies);
            mockRepository.Setup(x => x.GetById(2)).Returns(companies[1]);
            mockRepository.Setup(x => x.Insert(It.IsAny<Company>())).Returns((Company c) => c);
            companyService = new CompanyService(mockRepository.Object);
        }

        [Fact]
        public void ReturnsAllCompanies()
        {
            var result = companyService.GetCompanies();

            Assert.IsType<List<CompanyDto>>(result);
            Assert.Equal(3, result.Count);
        }

        [Fact]
        public void ReturnsCompanyByID()
        {
            var result = companyService.GetCompany(2);

            Assert.IsType<CompanyDto>(result);
            Assert.Equal(2, ((CompanyDto)result).ID);
        }

        [Fact]
        public void ReturnsNullWhenGettingByNonexistingId()
        {
            var result = companyService.GetCompany(500);

            Assert.Null(result);
        }

        [Fact]
        public void AddsCompany()
        {
            var mockRepository = new Mock<ICompaniesRepository>();
            mockRepository.Setup(x => x.Insert(It.IsAny<Company>()))
                .Returns((Company c) => new Company() { ID = 4, FullName = c.FullName, ShortName = c.ShortName });
            CompanyService service = new CompanyService(mockRepository.Object);

            CompanyDto dto = new CompanyDto()
            {
                FullName = "Company 4",
                ShortName = "Cny4"
            };
            var result = service.SaveCompany(dto);

            mockRepository.Verify(x => x.Insert(It.IsAny<Company>()), Times.Once);
            Assert.IsType<CompanyDto>(result);
            Assert.Equal(dto.FullName, result.FullName);
            Assert.Equal(dto.ShortName, result.ShortName);
            Assert.NotNull(result.ID);
        }

        [Fact]
        public void AddingNullReturnsNull()
        {
            var mockRepository = new Mock<ICompaniesRepository>();
            CompanyService service = new CompanyService(mockRepository.Object);

            var result = service.SaveCompany(null);

            mockRepository.Verify(x => x.Insert(It.IsAny<Company>()), Times.Never);
            Assert.Null(result);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData(" ")]
        public void AddingWithoutFullNameThrowsException(string fullName)
        {
            var mockRepository = new Mock<ICompaniesRepository>();
            CompanyService service = new CompanyService(mockRepository.Object);

            CompanyDto dto = new CompanyDto()
            {
                FullName = fullName,
                ShortName = "Cny4"
            };

            Assert.Throws<ArgumentException>(() => service.SaveCompany(dto));
            mockRepository.Verify(x => x.Insert(It.IsAny<Company>()), Times.Never);            
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData(" ")]
        public void AddingWithoutShortNameThrowsException(string shortName)
        {
            var mockRepository = new Mock<ICompaniesRepository>();
            CompanyService service = new CompanyService(mockRepository.Object);

            CompanyDto dto = new CompanyDto()
            {
                FullName = "Company 4",
                ShortName = shortName
            };

            Assert.Throws<ArgumentException>(() => service.SaveCompany(dto));
            mockRepository.Verify(x => x.Insert(It.IsAny<Company>()), Times.Never);
        }

        [Fact]
        public void UpdatesCompany()
        {
            var mockRepository = new Mock<ICompaniesRepository>();
            mockRepository.Setup(x => x.Update(It.IsAny<Company>()))
                .Returns((Company c) => c);
            CompanyService service = new CompanyService(mockRepository.Object);

            CompanyDto dto = new CompanyDto()
            {
                ID = 2,
                FullName = "Company 2 new name",
                ShortName = "Cny2 new"
            };
            var result = service.SaveCompany(dto);

            mockRepository.Verify(x => x.Update(It.IsAny<Company>()), Times.Once);
            mockRepository.Verify(x => x.Insert(It.IsAny<Company>()), Times.Never);
            Assert.IsType<CompanyDto>(result);
            Assert.Equal(dto.FullName, result.FullName);
            Assert.Equal(dto.ShortName, result.ShortName);
            Assert.Equal(dto.ID, result.ID);
        }
    }
}