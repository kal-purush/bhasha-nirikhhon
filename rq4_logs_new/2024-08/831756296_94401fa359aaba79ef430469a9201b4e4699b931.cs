using API.Controllers;
using API.Middlewares.Exceptions;
using Application.Services;
using Application.ViewModels;
using FluentAssertions;
using Microsoft.AspNetCore.Mvc;
using Moq;
using Tests.Fixtures;

namespace Tests.Controllers
{
    public class ContactControllerTests
    {
        private readonly ContactsController _contactsController;
        private readonly Mock<IContactService> _mockContactService;

        public ContactControllerTests()
        {
            _mockContactService = new Mock<IContactService>();
            _contactsController = new ContactsController(_mockContactService.Object);
        }

        [Fact(DisplayName = "Lista todos contatos")]
        [Trait("Metodo", "ListAllContacts")]
        public async void ListAllContacts_ShouldReturnAllContacts_AndStatusCode200()
        {
            // Arrange
            var contact1 = ContactFixture.CreateFakeContactResponseModel();
            var contact2 = ContactFixture.CreateFakeContactResponseModel();

            var contacts = new List<ContactResponseModel>()
            {
                contact1, contact2
            };

            _mockContactService.Setup(serv => serv.ListAll()).ReturnsAsync(contacts);

            // Act
            var result = await _contactsController.ListAllContacts();

            //Assert
            var actionResult = result.Result.Should().BeOfType<OkObjectResult>().Subject;
            actionResult.StatusCode.Should().Be(200);
            var returnedContacts = actionResult.Value.Should().BeAssignableTo<IList<ContactResponseModel>>().Subject;
            returnedContacts.Should().HaveCount(2);
            _mockContactService.Verify(serv => serv.ListAll(), Times.Once);
        }

        [Fact(DisplayName = "Lista contatos com filtro regiao")]
        [Trait("Metodo", "ListAllContacts")]
        public async void ListAllContacts_ShouldReturnFilteredContacts_AndStatusCode200()
        {
            // Arrange
            var contact1 = ContactFixture.CreateFakeContactResponseModel();
            var contact2 = ContactFixture.CreateFakeContactResponseModel();
            var contact3 = ContactFixture.CreateFakeContactResponseModel();

            contact1.Ddd = "14";
            contact2.Ddd = "14";
            contact3.Ddd = "11";

            var contacts = new List<ContactResponseModel>()
            {
                contact1, contact2
            };

            _mockContactService.Setup(serv => serv.FilterByDdd("14")).ReturnsAsync(contacts);

            // Act
            var result = await _contactsController.ListAllContacts("14");

            //Assert
            var actionResult = result.Result.Should().BeOfType<OkObjectResult>().Subject;
            actionResult.StatusCode.Should().Be(200);
            var returnedContacts = actionResult.Value.Should().BeAssignableTo<IList<ContactResponseModel>>().Subject;
            returnedContacts.Should().HaveCount(2);
            _mockContactService.Verify(serv => serv.ListAll(), Times.Never);
            _mockContactService.Verify(serv => serv.FilterByDdd(It.IsAny<string>()), Times.Once);
        }

        [Fact(DisplayName = "Lista contatos caso sistema sem contatos cadastrados")]
        [Trait("Metodo", "ListAllContacts")]
        public async void ListAllContacts_ShouldReturnEmptyList_WhenNoContactRegistered_AndStatusCode200()
        {
            // Arrange
            List<ContactResponseModel> emptyList = [];
            _mockContactService.Setup(serv => serv.ListAll()).ReturnsAsync(emptyList);

            // Act
            var result = await _contactsController.ListAllContacts();

            //Assert
            var actionResult = result.Result.Should().BeOfType<OkObjectResult>().Subject;
            actionResult.StatusCode.Should().Be(200);
            var returnedContacts = actionResult.Value.Should().BeAssignableTo<IList<ContactResponseModel>>().Subject;
            returnedContacts.Should().HaveCount(0);
            _mockContactService.Verify(serv => serv.ListAll(), Times.Once);
        }

        [Fact(DisplayName = "Consulta contato pelo Id")]
        [Trait("Metodo", "GetContact")]
        public async void GetContact_ShouldReturnContactById_AndStatusCode200()
        {
            // Arrange
            var contact1 = ContactFixture.CreateFakeContactResponseModel();
            contact1.Id = 1;

            _mockContactService.Setup(serv => serv.GetById(contact1.Id)).ReturnsAsync(contact1);

            // Act
            var result = await _contactsController.GetContact(1);

            //Assert
            var actionResult = result.Result.Should().BeOfType<OkObjectResult>().Subject;
            actionResult.StatusCode.Should().Be(200);
            var returnedContact = actionResult.Value.Should().BeAssignableTo<ContactResponseModel>().Subject;
            _mockContactService.Verify(serv => serv.GetById(It.IsAny<int>()), Times.Once);
        }

        [Fact(DisplayName = "Consulta contato por Id nao encontrado")]
        [Trait("Metodo", "GetContact")]
        public async void GetContact_ShouldReturnNotFound_WhenIdDoesNotExist()
        {
            // Arrange
            var contact1 = ContactFixture.CreateFakeContactResponseModel();
            contact1.Id = 1;

            int nonExistentId = 99;

            _mockContactService.Setup(serv => serv.GetById(nonExistentId)).ReturnsAsync((ContactResponseModel)null);

            // Act
            Func<Task> act = async () => await _contactsController.GetContact(nonExistentId);

            // Assert
            await act.Should().ThrowAsync<NotFoundException>();
            _mockContactService.Verify(serv => serv.GetById(It.IsAny<int>()), Times.Once);
        }

        [Fact(DisplayName = "Cadastro novo contato sucesso")]
        [Trait("Metodo", "CreateContact")]
        public async void CreateContact_ShouldReturnCreatedContact_AndStatusCode201_WhenNewContactCreated()
        {
            // Arrange
            var contactReq = ContactFixture.CreateFakeContactRequestModel();

            var contactResp = ContactFixture.CreateFakeContactResponseModel();

            _mockContactService.Setup(serv => serv.Create(contactReq)).ReturnsAsync(contactResp);

            // Act
            var result = await _contactsController.CreateContact(contactReq);

            //Assert
            var actionResult = result.Result.Should().BeOfType<CreatedAtActionResult>().Subject;
            actionResult.StatusCode.Should().Be(201);
            var returnedContact = actionResult.Value.Should().BeAssignableTo<ContactResponseModel>().Subject;
            returnedContact.Should().BeEquivalentTo(contactResp);
            _mockContactService.Verify(serv => serv.Create(It.IsAny<ContactRequestModel>()), Times.Once);
        }

        [Fact(DisplayName = "Erro nome invalido cadastro novo contato")]
        [Trait("Metodo", "CreateContact")]
        public async void CreateContact_ShouldReturnBadRequest_WhenReceiveInvalidName()
        {
            // Arrange
            var contactReq = ContactFixture.CreateFakeContactRequestModel();
            contactReq.Name = "S";

            // Act
            Func<Task> act = async () => await _contactsController.CreateContact(contactReq);

            //Assert
            await act.Should().ThrowAsync<DataValidationException>();
        }

        [Fact(DisplayName = "Erro email invalido cadastro novo contato")]
        [Trait("Metodo", "CreateContact")]
        public async void CreateContact_ShouldReturnBadRequest_WhenReceiveInvalidEmail()
        {
            // Arrange
            var contactReq = ContactFixture.CreateFakeContactRequestModel();
            contactReq.Email = "teste.com";

            // Act
            Func<Task> act = async () => await _contactsController.CreateContact(contactReq);

            //Assert
            await act.Should().ThrowAsync<DataValidationException>();
        }

        [Fact(DisplayName = "Erro Ddd invalido cadastro novo contato")]
        [Trait("Metodo", "CreateContact")]
        public async void CreateContact_ShouldReturnBadRequest_WhenReceiveInvalidDdd()
        {
            // Arrange
            var contactReq = ContactFixture.CreateFakeContactRequestModel();
            contactReq.Ddd = "123";

            // Act
            Func<Task> act = async () => await _contactsController.CreateContact(contactReq);

            //Assert
            await act.Should().ThrowAsync<DataValidationException>();
        }

        [Fact(DisplayName = "Erro telefone invalido cadastro novo contato")]
        [Trait("Metodo", "CreateContact")]
        public async void CreateContact_ShouldReturnBadRequest_WhenReceiveInvalidPhone()
        {
            // Arrange
            var contactReq = ContactFixture.CreateFakeContactRequestModel();
            contactReq.Phone = "A9852-4256";

            // Act
            Func<Task> act = async () => await _contactsController.CreateContact(contactReq);

            //Assert
            await act.Should().ThrowAsync<DataValidationException>();
        }

        [Fact(DisplayName = "Atualizacao contato sucesso")]
        [Trait("Metodo", "PutContact")]
        public async void PutContact_ShouldReturnUpdatedContact_AndStatusCode200_WhenNewContactUpdated()
        {
            // Arrange
            var contactReq = ContactFixture.CreateFakeContactRequestModel();

            var contactResp = ContactFixture.CreateFakeContactResponseModel();
            contactResp.Id = 1;

            _mockContactService.Setup(serv => serv.Update(1, contactReq)).ReturnsAsync(contactResp);

            // Act
            var result = await _contactsController.PutContact(1, contactReq);

            //Assert
            var actionResult = result.Result.Should().BeOfType<OkObjectResult>().Subject;
            actionResult.StatusCode.Should().Be(200);
            var returnedContact = actionResult.Value.Should().BeAssignableTo<ContactResponseModel>().Subject;
            returnedContact.Should().BeEquivalentTo(contactResp);
            _mockContactService.Verify(serv => serv.Update(It.IsAny<int>(), It.IsAny<ContactRequestModel>()), Times.Once);
        }

        [Fact(DisplayName = "Erro nome invalido atualizacao contato")]
        [Trait("Metodo", "PutContact")]
        public async void PutContact_ShouldReturnBadRequest_WhenReceiveInvalidName()
        {
            // Arrange
            var contactReq = ContactFixture.CreateFakeContactRequestModel();
            contactReq.Name = "S";

            // Act
            Func<Task> act = async () => await _contactsController.PutContact(1, contactReq);

            //Assert
            await act.Should().ThrowAsync<DataValidationException>();
        }

        [Fact(DisplayName = "Erro email invalido atualizacao contato")]
        [Trait("Metodo", "PutContact")]
        public async void PutContact_ShouldReturnBadRequest_WhenReceiveInvalidEmail()
        {
            // Arrange
            var contactReq = ContactFixture.CreateFakeContactRequestModel();
            contactReq.Email = "teste.com";

            // Act
            Func<Task> act = async () => await _contactsController.PutContact(1, contactReq);

            //Assert
            await act.Should().ThrowAsync<DataValidationException>();
        }

        [Fact(DisplayName = "Erro Ddd invalido atualizacao contato")]
        [Trait("Metodo", "PutContact")]
        public async void PutContact_ShouldReturnBadRequest_WhenReceiveInvalidDdd()
        {
            // Arrange
            var contactReq = ContactFixture.CreateFakeContactRequestModel();
            contactReq.Ddd = "123";

            // Act
            Func<Task> act = async () => await _contactsController.PutContact(1, contactReq);

            //Assert
            await act.Should().ThrowAsync<DataValidationException>();
        }

        [Fact(DisplayName = "Erro telefone invalido atualizacao contato")]
        [Trait("Metodo", "PutContact")]
        public async void PutContact_ShouldReturnBadRequest_WhenReceiveInvalidPhone()
        {
            // Arrange
            var contactReq = ContactFixture.CreateFakeContactRequestModel();
            contactReq.Phone = "A9852-4256";

            // Act
            Func<Task> act = async () => await _contactsController.PutContact(1, contactReq);

            //Assert
            await act.Should().ThrowAsync<DataValidationException>();
        }

        [Fact(DisplayName = "Atualizacao contato com Id nao encontrado")]
        [Trait("Metodo", "PutContact")]
        public async void PutContact_ShouldReturnNotFound_WhenIdDoesNotExist()
        {
            // Arrange
            var contactReq = ContactFixture.CreateFakeContactRequestModel();

            int nonExistentId = 99;

            _mockContactService.Setup(serv => serv.Update(nonExistentId, contactReq)).ReturnsAsync((ContactResponseModel)null);

            // Act
            Func<Task> act = async () => await _contactsController.PutContact(nonExistentId, contactReq);

            // Assert
            await act.Should().ThrowAsync<NotFoundException>();
            _mockContactService.Verify(serv => serv.Update(It.IsAny<int>(), It.IsAny<ContactRequestModel>()), Times.Once);
        }

        [Fact(DisplayName = "Deleta contato com sucesso")]
        [Trait("Metodo", "DeleteContact")]
        public async void DeleteContact_ShouldReturn204_WhenExecuteWithSuccess()
        {
            // Arrange
            _mockContactService.Setup(serv => serv.Delete(1)).ReturnsAsync(true);

            // Act
            var result = await _contactsController.DeleteContact(1);

            //Assert
            var actionResult = result.Should().BeOfType<NoContentResult>().Subject;
            actionResult.StatusCode.Should().Be(204);
            _mockContactService.Verify(serv => serv.Delete(It.IsAny<int>()), Times.Once);
        }

        [Fact(DisplayName = "Delete contato com Id nao encontrado")]
        [Trait("Metodo", "DeleteContact")]
        public async void DeleteContact_ShouldReturnNotFound_WhenIdDoesNotExist()
        {
            // Arrange
            int nonExistentId = 99;
            _mockContactService.Setup(serv => serv.Delete(nonExistentId)).ReturnsAsync(false);

            // Act
            Func<Task> act = async () => await _contactsController.DeleteContact(nonExistentId);

            // Assert
            await act.Should().ThrowAsync<NotFoundException>();
            _mockContactService.Verify(serv => serv.Delete(It.IsAny<int>()), Times.Once);
        }

    }
}