using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FluentAssertions;
using HelpI.API.Application.Services.Application;
using HelpI.API.Application.Services.Security;
using HelpI.API.Domain.Models.Application;
using HelpI.API.Domain.Models.Security;
using HelpI.API.Domain.Services.Communications;
using Moq;
using NUnit.Framework;

namespace HelpI.API.Test.IntegrationTests
{
    public class ExpertApplicationServiceTest : BaseServiceTest
    {
        [SetUp]
        public void Setup()
        {
            
        }

        //GetAllAsync
        [Test]
        public async Task GetAllAsyncWhenNoExpertApplicationsReturnEmptyCollection()
        {
            // Arrange
            var mockExpertApplicationRepository = GetDefaultIExpertApplicationRepositoryInstance();
            var mockExpertRepository = GetDefaultIExpertRepositoryInstance();
            var mockPlayerRepository = GetDefaultIPlayerRepositoryInstance();
            var mockUnitOfWork = GetDefaultIUnitOfWorkInstance();

            mockExpertApplicationRepository.Setup(r => r.ListAsync()).ReturnsAsync(new List<ExpertApplication>());
            
            var service = new ExpertApplicationService(mockExpertApplicationRepository.Object, mockUnitOfWork.Object, mockPlayerRepository.Object, mockExpertRepository.Object);

            // Act

            List<ExpertApplication> result = (List<ExpertApplication>)await service.ListAsync();
            var expertApplicationsCount = result.Count;

            // Assert
            expertApplicationsCount.Should().Equals(0);
        }
        
        //GetByIdAsync
        [Test]
        public async Task GetByIdAsyncWhenInvalidIdReturnsExpertApplicationNotFoundResponse()
        {
            // Arrange
            var mockExpertApplicationRepository = GetDefaultIExpertApplicationRepositoryInstance();
            var mockExpertRepository = GetDefaultIExpertRepositoryInstance();
            var mockPlayerRepository = GetDefaultIPlayerRepositoryInstance();
            var mockUnitOfWork = GetDefaultIUnitOfWorkInstance();
            var expertApplicationId = 1;
            
            mockExpertApplicationRepository.Setup(r => r.FindById(expertApplicationId)).Returns(Task.FromResult<ExpertApplication>(null));
            var service = new ExpertApplicationService(mockExpertApplicationRepository.Object, mockUnitOfWork.Object, mockPlayerRepository.Object, mockExpertRepository.Object);

            // Act
            ExpertApplicationResponse result = await service.GetByIdAsync(expertApplicationId);
            var message = result.Message;

            //Assert
            message.Should().Be("Application not found");
        }
        
        //DeleteAsync
        [Test]
        public async Task DeleteAsyncWhenExpertApplicationDoesNotExistsReturnsApplicationNotFoundResponse()
        {
            // Arrange
            var mockExpertApplicationRepository = GetDefaultIExpertApplicationRepositoryInstance();
            var mockExpertRepository = GetDefaultIExpertRepositoryInstance();
            var mockPlayerRepository = GetDefaultIPlayerRepositoryInstance();
            var mockUnitOfWork = GetDefaultIUnitOfWorkInstance();
            var expertApplicationId = -1;

            mockExpertApplicationRepository.Setup(r => r.FindById(expertApplicationId)).Returns(Task.FromResult<ExpertApplication>(null));
            var service = new ExpertApplicationService(mockExpertApplicationRepository.Object, mockUnitOfWork.Object, mockPlayerRepository.Object, mockExpertRepository.Object);

            // Act
            ExpertApplicationResponse result = await service.GetByIdAsync(expertApplicationId);
            var message = result.Message;

            //Assert
            message.Should().Be("Application not found");
        }
        
        //ListByApplicantId
        [Test]
        public async Task ListByApplicantIdAsyncWhenNoExpertApplicationsReturnEmptyCollection()
        {
            // Arrange
            var mockExpertApplicationRepository = GetDefaultIExpertApplicationRepositoryInstance();
            var mockExpertRepository = GetDefaultIExpertRepositoryInstance();
            var mockPlayerRepository = GetDefaultIPlayerRepositoryInstance();
            var mockUnitOfWork = GetDefaultIUnitOfWorkInstance();
            var applicantId = -1;

            mockExpertApplicationRepository.Setup(r => r.ListByApplicantIdAsync(applicantId))
                .ReturnsAsync(new List<ExpertApplication>());
            
            var service = new ExpertApplicationService(mockExpertApplicationRepository.Object, mockUnitOfWork.Object, mockPlayerRepository.Object, mockExpertRepository.Object);

            // Act
            List<ExpertApplication> result = (List<ExpertApplication>)await service.ListByApplicantIdAsync(applicantId);
            var expertApplicationsCount = result.Count;

            // Assert
            expertApplicationsCount.Should().Equals(0);
        }
        
        //SendExpertApplication
        [Test]
        public async Task SendExpertApplicationWhenApplicantDoesNotExistsReturnApplicantNotFound()
        {
            // Arrange
            var mockExpertApplicationRepository = GetDefaultIExpertApplicationRepositoryInstance();
            var mockExpertRepository = GetDefaultIExpertRepositoryInstance();
            var mockPlayerRepository = GetDefaultIPlayerRepositoryInstance();
            var mockUnitOfWork = GetDefaultIUnitOfWorkInstance();
            var applicantId = -1;

            mockPlayerRepository.Setup(r => r.FindById(applicantId)).Returns(Task.FromResult<Player>(null));
            var service = new ExpertApplicationService(mockExpertApplicationRepository.Object, mockUnitOfWork.Object, mockPlayerRepository.Object, mockExpertRepository.Object);
            
            // Act
            ExpertApplicationResponse result = await service.SendExpertApplication(applicantId, new ExpertApplication());
            var message = result.Message;

            // Assert
            message.Should().Be("Applicant Not Found");

        }
        
        //ReviewApplicationAsync
        [Test]
        public async Task ReviewApplicationAsyncWhenApplicationDoesNotExistsReturnApplicationNotFound()
        {
            // Arrange
            var mockExpertApplicationRepository = GetDefaultIExpertApplicationRepositoryInstance();
            var mockExpertRepository = GetDefaultIExpertRepositoryInstance();
            var mockPlayerRepository = GetDefaultIPlayerRepositoryInstance();
            var mockUnitOfWork = GetDefaultIUnitOfWorkInstance();
            var expertApplicationId = -1;

            mockExpertApplicationRepository.Setup(r => r.FindById(expertApplicationId))
                .Returns(Task.FromResult<ExpertApplication>(null));

            var service = new ExpertApplicationService(mockExpertApplicationRepository.Object, mockUnitOfWork.Object, mockPlayerRepository.Object, mockExpertRepository.Object);
            
            // Act
            ExpertResponse result = await service.ReviewApplicationAsync(expertApplicationId, "", "");
            var message = result.Message;

            // Assert
            message.Should().Be("Application not found");
        }

        [Test]
        public async Task ReviewApplicationAsyncWhenApplicationRejectedReturnsApplicationHasAlreadyBeenRejected()
        {
            // Arrange
            var mockExpertApplicationRepository = GetDefaultIExpertApplicationRepositoryInstance();
            var mockExpertRepository = GetDefaultIExpertRepositoryInstance();
            var mockPlayerRepository = GetDefaultIPlayerRepositoryInstance();
            var mockUnitOfWork = GetDefaultIUnitOfWorkInstance();
            
            var expertApplicationId = 1;
            ExpertApplication expertApplication = new ExpertApplication();
            expertApplication.Id = expertApplicationId;
            ApplicationDetail applicationDetail = new ApplicationDetail("", new Uri("http://www.example.com/"), EApplicationStatus.Rejected,"");
            expertApplication.ApplicationDetails = applicationDetail;

            mockExpertApplicationRepository.Setup(r => r.FindById(expertApplicationId))
                .Returns(Task.FromResult(expertApplication));
            
            var service = new ExpertApplicationService(mockExpertApplicationRepository.Object, mockUnitOfWork.Object, mockPlayerRepository.Object, mockExpertRepository.Object);
            // Act
            ExpertResponse result = await service.ReviewApplicationAsync(expertApplicationId, "", "");
            var message = result.Message;

            // Assert
            message.Should().Be("Application has already been rejected");

        }
        
        [Test]
        public async Task ReviewApplicationAsyncWhenApplicationAcceptedReturnsApplicationHasAlreadyBeenAccepted()
        {
            // Arrange
            var mockExpertApplicationRepository = GetDefaultIExpertApplicationRepositoryInstance();
            var mockExpertRepository = GetDefaultIExpertRepositoryInstance();
            var mockPlayerRepository = GetDefaultIPlayerRepositoryInstance();
            var mockUnitOfWork = GetDefaultIUnitOfWorkInstance();
            
            var expertApplicationId = 1;
            ExpertApplication expertApplication = new ExpertApplication();
            expertApplication.Id = expertApplicationId;
            ApplicationDetail applicationDetail = new ApplicationDetail("", new Uri("http://www.example.com/"), EApplicationStatus.Passed,"");
            expertApplication.ApplicationDetails = applicationDetail;

            mockExpertApplicationRepository.Setup(r => r.FindById(expertApplicationId))
                .Returns(Task.FromResult(expertApplication));
            
            var service = new ExpertApplicationService(mockExpertApplicationRepository.Object, mockUnitOfWork.Object, mockPlayerRepository.Object, mockExpertRepository.Object);
            // Act
            ExpertResponse result = await service.ReviewApplicationAsync(expertApplicationId, "", "");
            var message = result.Message;

            // Assert
            message.Should().Be("Application has already been accepted");
            
        }

        [Test]
        public async Task ReviewApplicationAsyncWhenRejectApplicationReturnsApplicationHasBeenRejected()
        {
            // Arrange
            var mockExpertApplicationRepository = GetDefaultIExpertApplicationRepositoryInstance();
            var mockExpertRepository = GetDefaultIExpertRepositoryInstance();
            var mockPlayerRepository = GetDefaultIPlayerRepositoryInstance();
            var mockUnitOfWork = GetDefaultIUnitOfWorkInstance();
            
            var expertApplicationId = 1;
            ExpertApplication expertApplication = new ExpertApplication();
            expertApplication.Id = expertApplicationId;
            ApplicationDetail applicationDetail = new ApplicationDetail("", new Uri("http://www.example.com/"), EApplicationStatus.Pending,"");
            expertApplication.ApplicationDetails = applicationDetail;

            mockExpertApplicationRepository.Setup(r => r.FindById(expertApplicationId))
                .Returns(Task.FromResult(expertApplication));
            
            var service = new ExpertApplicationService(mockExpertApplicationRepository.Object, mockUnitOfWork.Object, mockPlayerRepository.Object, mockExpertRepository.Object);
            // Act
            ExpertResponse result = await service.ReviewApplicationAsync(expertApplicationId, "Rejected", "");
            var message = result.Message;

            // Assert
            message.Should().Be("The application has been rejected");
        }

        [Test]
        public async Task ReviewApplicationAsyncWhenPassedApplicationReturnsNewExpert()
        {
            // Arrange
            var mockExpertApplicationRepository = GetDefaultIExpertApplicationRepositoryInstance();
            var mockExpertRepository = GetDefaultIExpertRepositoryInstance();
            var mockPlayerRepository = GetDefaultIPlayerRepositoryInstance();
            var mockUnitOfWork = GetDefaultIUnitOfWorkInstance();
            
            var expertApplicationId = 1;
            ExpertApplication expertApplication = new ExpertApplication();
            expertApplication.Id = expertApplicationId;
            ApplicationDetail applicationDetail = new ApplicationDetail("", new Uri("http://www.example.com/"), EApplicationStatus.Pending,"");
            expertApplication.ApplicationDetails = applicationDetail;
            Player player = new Player();
            var playerId = 1;
            player.Id = playerId;
            expertApplication.Applicant = player;

            mockExpertApplicationRepository.Setup(r => r.FindById(expertApplicationId))
                .Returns(Task.FromResult(expertApplication));
            
            mockPlayerRepository.Setup(r => r.FindById(expertApplication.PlayerId))
                .Returns(Task.FromResult(player));
            
            var service = new ExpertApplicationService(mockExpertApplicationRepository.Object, mockUnitOfWork.Object, mockPlayerRepository.Object, mockExpertRepository.Object);
            // Act
            ExpertResponse result = await service.ReviewApplicationAsync(expertApplicationId, "Passed", "");

            // Assert
            result.Resource.Should().BeEquivalentTo(new Expert(player));
        }
    }
}