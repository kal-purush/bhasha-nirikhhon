using FluentAssertions;
using Moq;
using MRF.API.Controllers;
using MRF.API.Test.Controllers;
using MRF.Models.DTO;
using MRF.Models.Models;
using System.Linq.Expressions;

namespace MRF.API.Test.Controller
{
    public class CandidateInterviewFeedbackControllerTest
    {
        private readonly TestFixture fixture;
        private CandidateInterviewFeedbackController Controller;
        public CandidateInterviewFeedbackControllerTest()
        {
            fixture = new TestFixture();
            Controller = new CandidateInterviewFeedbackController(fixture.MockUnitOfWork.Object, fixture.MockLogger.Object);
        }

        [Fact]
        public void CandidateInterviewFeedbackController_ShouldReturnCount_WhenDataFound()
        {
            // Create a list of sample CandidateInterviewFeedback for testing
            var SampleCandidateInterviewFeedback = new List<CandidateInterviewFeedback> {
        new CandidateInterviewFeedback {
          Id = 1, CandidateId = 1, SoftSkills = "Java", HardSkills = "Quick Learner", RequiredTraining = "Tomcat", Comments = "This is comment 1"
        },
        new CandidateInterviewFeedback {
          Id = 2, CandidateId = 1, SoftSkills = ".NET", HardSkills = "Team Player", RequiredTraining = "IIS", Comments = "This is comment 2"
        },
      };

            // Set up the behavior of the mockUnitOfWork to return the sample data
            fixture.MockUnitOfWork.Setup(uow => uow.CandidateInterviewFeedback.GetAll()).Returns(SampleCandidateInterviewFeedback);

            // Act
            var result = Controller.Get();

            // Assert
            Assert.Equal(SampleCandidateInterviewFeedback.Count, result.Count);
            Assert.NotNull(result);
            result.Should().NotBeNull();
            fixture.MockUnitOfWork.Verify(x => x.CandidateInterviewFeedback, Times.Once());
        }

        [Fact]
        public void CandidateInterviewFeedbackController_ShouldReturnNotFound_WhenDataNotFound()
        {
            fixture.MockUnitOfWork.Setup(x => x.CandidateInterviewFeedback.GetAll()).Returns(new List<CandidateInterviewFeedback>());

            // Act
            var result = Controller.Get();

            // Assert
            result.Should().NotBeNull();
            fixture.MockLogger.Verify(logger => logger.LogError("No record is found"));
            fixture.MockUnitOfWork.Verify(x => x.CandidateInterviewFeedback, Times.Once());
        }

        [Fact]
        public void GetCandidateInterviewFeedbackById_ShouldReturnNoResult_WhenInputIsEqualsZero()
        {
            // Arrange
            int id = 0;

            // Create a list of sample CandidateInterviewFeedback for testing
            var sampleCandidateInterviewFeedback = new List<CandidateInterviewFeedback> {
        new CandidateInterviewFeedback {
          Id = 1, CandidateId = 1, SoftSkills = "Java", HardSkills = "Quick Learner", RequiredTraining = "Tomcat", Comments = "This is comment 1"
        },
        new CandidateInterviewFeedback {
          Id = 2, CandidateId = 1, SoftSkills = ".NET", HardSkills = "Team Player", RequiredTraining = "IIS", Comments = "This is comment 2"
        },
        // Add more sample data as needed
      };

            // Set up the behavior of the mockUnitOfWork to return the sample data
            fixture.MockUnitOfWork.Setup(uow => uow.CandidateInterviewFeedback.GetAll()).Returns(sampleCandidateInterviewFeedback);

            // Act
            var result = Controller.Get(id);

            // Assert
            result.Should().NotBeNull();
            fixture.MockLogger.Verify(logger => logger.LogError("No result found by this Id:0"));
        }

        [Fact]
        public void GetCandidateInterviewFeedbackById_ShouldReturnNoResult_WhenInputIsLessThanZero()
        {
            // Arrange
            int id = 0;

            // Create a list of sample CandidateInterviewFeedback for testing
            var sampleCandidateInterviewFeedback = new List<CandidateInterviewFeedback> {
        new CandidateInterviewFeedback {
          Id = 1, CandidateId = 1, SoftSkills = "Java", HardSkills = "Quick Learner", RequiredTraining = "Tomcat", Comments = "This is comment 1"
        },
        new CandidateInterviewFeedback {
          Id = 2, CandidateId = 1, SoftSkills = ".NET", HardSkills = "Team Player", RequiredTraining = "IIS", Comments = "This is comment 2"
        },
        // Add more sample data as needed   
      };

            // Set up the behavior of the mockUnitOfWork to return the sample data
            fixture.MockUnitOfWork.Setup(uow => uow.CandidateInterviewFeedback.GetAll()).Returns(sampleCandidateInterviewFeedback);

            // Act  
            var result = Controller.Get(id);

            // Assert
            result.Should().NotBeNull();
            fixture.MockLogger.Verify(logger => logger.LogError("No result found by this Id:0"));
        }

        [Fact]
        public void CreateCandidateInterviewFeedbackdetail_ShouldReturnOkResponse_WhenValidRequest()
        {

            var requestModel = new CandidateInterviewFeedbackRequestModel
            {
                CandidateId = 1,
                SoftSkills = "Java",
                HardSkills = ".NET",
                RequiredTraining = "Azure",
                Comments = "Good Resource",
                CreatedByEmployeeId = 1,
                CreatedOnUtc = DateTime.Now,
                UpdatedByEmployeeId = 1,

            };

            // Mock the behavior of IUnitOfWork
            fixture.MockUnitOfWork.Setup(uow => uow.CandidateInterviewFeedback.Add(It.IsAny<CandidateInterviewFeedback>()));
            fixture.MockUnitOfWork.Setup(uow => uow.Save()).Verifiable();

            // Create an instance of ResponseDTO to return
            var responseModel = new CandidateInterviewFeedbackResponseModel
            {
                Id = 0, // Set the expected Id
                        // Set other properties as needed
            };

            // Act
            var result = Controller.Post(requestModel);

            // Assert
            // Verify that the result is an OkObjectResult
            Assert.IsType<CandidateInterviewFeedbackResponseModel>(result);
            var okResult = (CandidateInterviewFeedbackResponseModel)result;

            // Check if the returned response matches the expected response
            Assert.Equal(responseModel.Id, okResult.Id); // Adjust the assertions for other properties

            // Verify that the CandidateInterviewFeedback was added and Save was called
            fixture.MockUnitOfWork.Verify(uow => uow.CandidateInterviewFeedback.Add(It.IsAny<CandidateInterviewFeedback>()), Times.Once);
            fixture.MockUnitOfWork.Verify(uow => uow.Save(), Times.Once);

        }
        [Fact]
        public void CreateCandidateInterviewFeedback_ShouldReturnBadRequest_WhenInvalidRequest()
        {
            // Mock the behavior of IUnitOfWork
            fixture.MockUnitOfWork.Setup(uow => uow.CandidateInterviewFeedback.Add(It.IsAny<CandidateInterviewFeedback>()));
            fixture.MockUnitOfWork.Setup(uow => uow.Save()).Verifiable();

            // Create an instance of ResponseDTO to return
            var responseModel = new CandidateInterviewFeedbackResponseModel
            {
                Id = 0, // Set the expected Id
                        // Set other properties as needed
            };

            // Act
            var result = Controller.Post(new CandidateInterviewFeedbackRequestModel());

            // Assert
            // Verify that the result is an OkObjectResult
            Assert.IsType<CandidateInterviewFeedbackResponseModel>(result);
            var okResult = (CandidateInterviewFeedbackResponseModel)result;

            // Check if the returned response matches the expected response
            Assert.Equal(responseModel.Id, okResult.Id); // Adjust the assertions for other properties

            // Verify that the CandidateInterviewFeedback was added and Save was called
            fixture.MockUnitOfWork.Verify(uow => uow.CandidateInterviewFeedback.Add(It.IsAny<CandidateInterviewFeedback>()), Times.Once);
            fixture.MockUnitOfWork.Verify(uow => uow.Save(), Times.Once);

            // Assert
            result.Should().NotBeNull();

        }
        [Fact]
        public void DeleteCandidateInterviewFeedback_ShouldReturnNoContents_WhenDeletedARecord()
        {
            // Arrange
            int existingId = 1; // Replace with an existing Id in your test data

            // Mock the behavior of the Get and Remove methods in IUnitOfWork
            fixture.MockUnitOfWork.Setup(uow => uow.CandidateInterviewFeedback.Get(It.IsAny<Expression<Func<CandidateInterviewFeedback, bool>>>()))
              .Returns((Expression<Func<CandidateInterviewFeedback, bool>> filter) => {
                  // Simulate returning an object when the filter condition matches
                  if (filter.Compile().Invoke(new CandidateInterviewFeedback
                  {
                      Id = existingId
                  }))
                  {
                      return new CandidateInterviewFeedback
                      {
                          Id = existingId
                      };
                  }
                  return null;
              });

            // Act
            Controller.Delete(existingId);

            // Assert
            // Verify that the Remove and Save methods of IUnitOfWork were called as expected
            fixture.MockUnitOfWork.Verify(uow => uow.CandidateInterviewFeedback.Get(It.IsAny<Expression<Func<CandidateInterviewFeedback, bool>>>()), Times.Once);
            fixture.MockUnitOfWork.Verify(uow => uow.CandidateInterviewFeedback.Remove(It.IsAny<CandidateInterviewFeedback>()), Times.Once);
            fixture.MockUnitOfWork.Verify(uow => uow.Save(), Times.Once);
        }
        [Fact]
        public void DeleteCandidateInterviewFeedback_ShouldReturnNotFound_WhenRecordNotFound()
        {
            // Arrange
            // Set up your mock IUnitOfWork behavior.
            int nonExistentId = 999; // An ID that is assumed not to exist.
            fixture.MockUnitOfWork.Setup(uow => uow.CandidateInterviewFeedback.Get(It.IsAny<Expression<Func<CandidateInterviewFeedback, bool>>>()))
              .Returns((Expression<Func<CandidateInterviewFeedback, bool>> predicate) => null);

            // Act
            Controller.Delete(nonExistentId);

            // Assert
            // Verify that the Remove and Save methods were not called since the object does not exist.
            fixture.MockUnitOfWork.Verify(uow => uow.CandidateInterviewFeedback.Remove(It.IsAny<CandidateInterviewFeedback>()), Times.Never);
            fixture.MockUnitOfWork.Verify(uow => uow.Save(), Times.Never);

        }
        [Fact]
        public void DeleteCandidateInterviewFeedback_ShouldReturnBadResponse_WhenInputIsZero()
        {
            // Arrange

            // Set up your mock IUnitOfWork behavior.
            int nonExistentId = 0;
            fixture.MockUnitOfWork.Setup(uow => uow.CandidateInterviewFeedback.Get(It.IsAny<Expression<Func<CandidateInterviewFeedback, bool>>>()))
              .Returns((Expression<Func<CandidateInterviewFeedback, bool>> predicate) => null);

            // Act
            Controller.Delete(nonExistentId);

            // Assert
            // Verify that the Remove and Save methods were not called since the object does not exist.
            fixture.MockUnitOfWork.Verify(uow => uow.CandidateInterviewFeedback.Remove(It.IsAny<CandidateInterviewFeedback>()), Times.Never);
            fixture.MockUnitOfWork.Verify(uow => uow.Save(), Times.Never);
        }
        [Fact]
        public void UpdateCandidateInterviewFeedback_ShouldReturnIsActiveTrue_WhenRecordIsUpdated()
        {
            // Arrange

            int entityId = 1; // Example ID for an existing entity

            var requestModel = new CandidateInterviewFeedbackRequestModel
            {
                CandidateId = 1,
                SoftSkills = "Java",
                HardSkills = ".NET",
                RequiredTraining = "Azure",
                Comments = "Good Resource",
                CreatedByEmployeeId = 1,
                CreatedOnUtc = DateTime.Now,
                UpdatedByEmployeeId = 1,

            };

            // Mock the behavior of IUnitOfWork's Get method to return an existing entity
            var existingEntity = new CandidateInterviewFeedback
            {
                Id = entityId
            };

            fixture.MockUnitOfWork.Setup(uow => uow.CandidateInterviewFeedback.Get(It.IsAny<Expression<Func<CandidateInterviewFeedback, bool>>>()))
              .Returns((Expression<Func<CandidateInterviewFeedback, bool>> predicate) => existingEntity);

            // Act
            var result = Controller.Put(entityId, requestModel);

            // Assert
            // Verify that the Update and Save methods were called and the response model contains the updated ID.
            fixture.MockUnitOfWork.Verify(uow => uow.CandidateInterviewFeedback.Update(It.IsAny<CandidateInterviewFeedback>()), Times.Once);
            fixture.MockUnitOfWork.Verify(uow => uow.Save(), Times.Once);

            Assert.Equal(entityId, result.Id);
        }
        [Fact]
        public void UpdateCandidateInterviewFeedback_ShouldReturnIsActiveFalse_WhenInvalidRequest()
        {
            // Arrange
            int entityId = 999; // Id for which the entity is not found

            var requestModel = new CandidateInterviewFeedbackRequestModel
            {
                CandidateId = 1,
                SoftSkills = "Java",
                HardSkills = ".NET",
                RequiredTraining = "Azure",
                Comments = "Good Resource",
                CreatedByEmployeeId = 1,
                CreatedOnUtc = DateTime.Now,
                UpdatedByEmployeeId = 1,

            };

            fixture.MockUnitOfWork.Setup(uow => uow.CandidateInterviewFeedback.Get(It.IsAny<Expression<Func<CandidateInterviewFeedback, bool>>>()))
              .Returns((Expression<Func<CandidateInterviewFeedback, bool>> predicate) => null);

            // Act
            var result = Controller.Put(entityId, requestModel);

            // Assert

            Assert.Equal(0, result.Id);

            // Verify that the Update and Save methods were not called since the entity does not exist.
            fixture.MockUnitOfWork.Verify(uow => uow.CandidateInterviewFeedback.Update(It.IsAny<CandidateInterviewFeedback>()), Times.Never);
            fixture.MockUnitOfWork.Verify(uow => uow.Save(), Times.Never);
        }
    }
}