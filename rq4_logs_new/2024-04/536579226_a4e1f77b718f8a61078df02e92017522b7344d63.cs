using System.Runtime.InteropServices;
using AutoFixture;
using Moq;
using PontoAPI.Core.Entities;
using PontoAPI.Core.Interface;
using PontoAPI.Infrastructure.Application;
using PontoAPI.Infrastructure.Test.Faker;
using Shouldly;

namespace PontoAPI.Infrastructure.Test.Application
{
    public class CompanyApplicationTests
    {

        // AAA - Arrange / Act / Assert
        // Given / When / then

        [Fact]
        public async void Get_Executed_Sucess()
        {
            //Arrange
            var companyFaker = CompanyFaker.GerarListaCompanyFake();
            var companyMock = new Mock<IRepository<Company>>();
            companyMock.Setup(x => x.Get()).ReturnsAsync(companyFaker);
            var comanyApplication = new CompanyApplication(companyMock.Object);

            //Act
            var companyResult = await comanyApplication.Get();

            // Assert
            companyResult.ShouldNotBeEmpty();
        }

        [Fact]
        public async void Get_Id_Executed_Sucess()
        {
            //Arrange
            var companyFaker = CompanyFaker.GerarCompanyFake();
            var companyMock = new Mock<IRepository<Company>>();
            companyMock.Setup(x => x.Get(companyFaker.Id)).ReturnsAsync(companyFaker);
            var comanyApplication = new CompanyApplication(companyMock.Object);

            //Act
            var companyResult = await comanyApplication.Get(companyFaker.Id);

            // Assert
            companyResult.ShouldNotBeNull();
        }

        [Fact]
        public async void Get_Id_Executed_NotFound()
        {
            //Arrange
            var companyFaker = CompanyFaker.GerarCompanyFake();
            var companyMock = new Mock<IRepository<Company>>();
            var guid = Guid.NewGuid();
            companyMock.Setup(x => x.Get(guid)).ReturnsAsync(companyFaker);
            var comanyApplication = new CompanyApplication(companyMock.Object);

            //Act
            var companyResult = await comanyApplication.Get(companyFaker.Id);

            // Assert
            companyResult.ShouldBeNull();
        }

        [Fact]
        public async void Post_Executed_Sucess()
        {
            //Arrange
            var companyFaker = CompanyFaker.GerarCompanyFake();
            var companyMock = new Mock<IRepository<Company>>();
            companyMock.Setup(x => x.Post(It.IsAny<Company>())).Verifiable();
            var comanyApplication = new CompanyApplication(companyMock.Object);

            //Act
            await comanyApplication.Post(companyFaker);
            await comanyApplication.SaveChangesAsync();
            companyMock.Setup(x => x.Get(companyFaker.Id)).ReturnsAsync(companyFaker);
            var companyDb = await comanyApplication.Get(companyFaker.Id);

            // Assert
            companyMock.VerifyAll();
            companyDb.ShouldNotBeNull();
        }

        [Fact]
        public async void Put_Executed_Sucess()
        {
            //Arrange
            var companyFaker = CompanyFaker.GerarCompanyFake();
            var companyMock = new Mock<IRepository<Company>>();
            companyMock.Setup(x => x.Get(companyFaker.Id)).ReturnsAsync(companyFaker);
            var comanyApplication = new CompanyApplication(companyMock.Object);

            //Act
            var newName = "Nome alterado";
            var newAddress = "Address new";
            var newTelephone = "11 1111-1111";
            companyFaker.Name = newName;
            companyFaker.Address = newAddress;
            companyFaker.Telephone = newTelephone;
            var companyResult = await comanyApplication.Put(companyFaker);
            await comanyApplication.SaveChangesAsync();

            // Assert
            companyMock.VerifyAll();
            Assert.Equal(newName, companyResult.Name);
            Assert.Equal(newAddress, companyResult.Address);
            Assert.Equal(newTelephone, companyResult.Telephone);
        }

        [Fact]
        public async void Delete_Executed_Sucess()
        {
            //Arrange
            var companyFaker = CompanyFaker.GerarCompanyFake();
            var companyMock = new Mock<IRepository<Company>>();
            companyMock.Setup(x => x.Delete(It.IsAny<Company>()));
            var comanyApplication = new CompanyApplication(companyMock.Object);

            //Act            
            comanyApplication.Delete(companyFaker);
            await comanyApplication.SaveChangesAsync();

            // Assert
            companyMock.Verify(x => x.Delete(It.IsAny<Company>()), Times.Once());
        }
    }
}