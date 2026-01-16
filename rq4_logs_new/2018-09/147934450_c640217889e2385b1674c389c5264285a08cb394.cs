using System;
using System.Collections.Generic;
using Microsoft.AspNetCore.Mvc;
using Xunit;
using Moq;
using Tapir.Controllers;
using Tapir.Core;

namespace Tapir.Tests.UnitTests.Controllers
{
    public class CompaniesControllerUnitTests
    {
        List<CompanyDto> companies = new List<CompanyDto>
        {
            new CompanyDto() { ID = 1, FullName = "Company1 Ltd", ShortName = "Ltd1"},
            new CompanyDto() { ID = 2, FullName = "Company2 Ltd", ShortName = "Ltd2"},
            new CompanyDto() { ID = 3, FullName = "Company3 Ltd", ShortName = "Ltd3"}
        };

        [Fact]
        public void GetAllReturnsAllCompanies()
        {
            var mockService = new Mock<ICompanyService>();
            mockService.Setup(s => s.GetCompanies()).Returns(companies);
            var controller = new CompaniesController(mockService.Object);

            var result = controller.GetAll();

            mockService.Verify(x => x.GetCompanies(), Times.Once);
            Assert.IsType<List<CompanyDto>>(result.Value);
            List<CompanyDto> resultList = (List<CompanyDto>)result.Value;
            Assert.Equal(companies.Count, resultList.Count);
            foreach (CompanyDto c in resultList)
            {
                Assert.NotNull(companies.Find(o => o.ID == c.ID));
            }
        }

        [Fact]
        public void GetByIdReturnsCompany()
        {
            var mockService = new Mock<ICompanyService>();
            mockService.Setup(s => s.GetCompany(2)).Returns(companies[1]);
            var controller = new CompaniesController(mockService.Object);

            var result = controller.GetById(2);

            mockService.Verify(x => x.GetCompany(2), Times.Once);
            Assert.IsType<CompanyDto>(result.Value);
            CompanyDto resultCo = (CompanyDto)result.Value;
            Assert.Equal(2, resultCo.ID);
        }

        [Fact]
        public void GetByIdReturnsNotFoundOnNonexistingId()
        {
            var mockService = new Mock<ICompanyService>();
            mockService.Setup(s => s.GetCompany(4)).Returns<CompanyDto>(null);
            var controller = new CompaniesController(mockService.Object);

            var result = controller.GetById(4);

            mockService.Verify(x => x.GetCompany(4), Times.Once);
            Assert.IsType<NotFoundResult>(result.Result);
        }
    }
}