using LT.DigitalOffice.Broker.Models;
using LT.DigitalOffice.Broker.Requests;
using LT.DigitalOffice.Broker.Responses;
using LT.DigitalOffice.Kernel.Broker;
using LT.DigitalOffice.Kernel.Exceptions.Models;
using LT.DigitalOffice.UnitTestKernel;
using LT.DigitalOffice.UserService.Business.Interfaces;
using LT.DigitalOffice.UserService.Data.Interfaces;
using LT.DigitalOffice.UserService.Mappers.Responses.Interfaces;
using LT.DigitalOffice.UserService.Models.Db;
using LT.DigitalOffice.UserService.Models.Dto;
using LT.DigitalOffice.UserService.Models.Dto.Models;
using LT.DigitalOffice.UserService.Models.Dto.Models.Certificates;
using LT.DigitalOffice.UserService.Models.Dto.Requests.User.Filters;
using LT.DigitalOffice.UserService.Models.Dto.Responses.User;
using MassTransit;
using Microsoft.Extensions.Logging;
using Moq;
using Moq.AutoMock;
using NUnit.Framework;
using System;
using System.Collections.Generic;

namespace LT.DigitalOffice.UserService.Business.UnitTests
{
    class GetUserCommandTests
    {
        private AutoMocker _mocker;
        private Mock<ILogger<GetUserCommand>> _loggerMock;
        private GetUserFilter _filter;
        private DbUser _dbUser;
        private UserResponse _userResponse;
        private IGetUserCommand _command;

        private Guid _userId = Guid.NewGuid();
        private Guid _departmentId = Guid.NewGuid();
        private Guid _positionId = Guid.NewGuid();
        private List<ProjectShortInfo> _projects;
        private Guid _imageId = Guid.NewGuid();


        [SetUp]
        public void SetUp()
        {
            _dbUser = new DbUser
            {
                Id = _userId,
                FirstName = "First",
                LastName = "Last",
                MiddleName = "Middle",
                Status = 1,
                AvatarFileId = Guid.NewGuid(),
                IsActive = true,
                About = "About",
                Rate = 1.3,
                CreatedAt = DateTime.UtcNow,
                StartWorkingAt = DateTime.UtcNow,
                Credentials = new DbUserCredentials(),
                Certificates = new List<DbUserCertificate>
                {
                    new DbUserCertificate
                    {
                        ImageId = _imageId
                    }
                },
                Achievements = new List<DbUserAchievement>(),
                Communications = new List<DbUserCommunication>(),
                Skills = new List<DbUserSkill>()
            };

            _filter = new GetUserFilter
            {
                UserId = _userId,
                Name = _dbUser.FirstName,
                Email = "email",
                IncludeCommunications = true,
                IncludeCertificates = true,
                IncludeAchievements = true,
                IncludeDepartment = true,
                IncludePosition = true,
                IncludeSkills = true,
                IncludeImages = true,
                IncludeProjects = true
            };

            _projects = new List<ProjectShortInfo>
            {
                new ProjectShortInfo
                {
                    Id = Guid.NewGuid(),
                    Name = "Project1"
                },
                new ProjectShortInfo
                {
                    Id = Guid.NewGuid(),
                    Name = "Project2"
                }
            };

            _userResponse = new UserResponse
            {
                User = new UserInfo(),
                Avatar = new ImageInfo(),
                Department = new DepartmentInfo(),
                Position = new PositionInfo(),
                Skills = new List<string>(),
                Communications = new List<CommunicationInfo>(),
                Certificates = new List<CertificateInfo>(),
                Achievements = new List<UserAchievementInfo>(),
                Projects = new List<ProjectInfo>(),
                Errors = new List<string>()
            };

            _mocker = new AutoMocker();

            _mocker
                .Setup<IUserRepository, DbUser>(x => x.Get(It.IsAny<GetUserFilter>()))
                .Returns(_dbUser);

            _mocker
                .Setup<IUserResponseMapper, UserResponse>(
                    x => x.Map(
                        It.IsAny<DbUser>(),
                        It.IsAny<DepartmentInfo>(),
                        It.IsAny<PositionInfo>(),
                        It.IsAny<List<ProjectInfo>>(),
                        It.IsAny<List<ImageInfo>>(),
                        It.IsAny<GetUserFilter>(),
                        It.IsAny<List<string>>()))
                .Returns(_userResponse);

            _mocker
                .Setup<IGetDepartmentResponse, Guid>(x => x.Id)
                .Returns(_departmentId);
            _mocker
                .Setup<IOperationResult<IGetDepartmentResponse>, bool>(x => x.IsSuccess)
                .Returns(true);
            _mocker
                .Setup<IOperationResult<IGetDepartmentResponse>, IGetDepartmentResponse>(x => x.Body)
                .Returns(_mocker.GetMock<IGetDepartmentResponse>().Object);
            _mocker
                .Setup<IRequestClient<IGetDepartmentRequest>, IOperationResult<IGetDepartmentResponse>>(
                    x => x.GetResponse<IOperationResult<IGetDepartmentResponse>>(IGetDepartmentRequest.CreateObj(
                        _userId, null),
                        default,
                        100)
                    .Result.Message)
                .Returns(_mocker.GetMock<IOperationResult<IGetDepartmentResponse>>().Object);

            _mocker
                .Setup<IPositionResponse, Guid>(x => x.Id)
                .Returns(_positionId);
            _mocker
                .Setup<IOperationResult<IPositionResponse>, bool>(x => x.IsSuccess)
                .Returns(true);
            _mocker
                .Setup<IOperationResult<IPositionResponse>, IPositionResponse>(x => x.Body)
                .Returns(_mocker.GetMock<IPositionResponse>().Object);
            _mocker
                .Setup<IRequestClient<IGetPositionRequest>, IOperationResult<IPositionResponse>>(
                    x => x.GetResponse<IOperationResult<IPositionResponse>>(IGetPositionRequest.CreateObj(
                        _userId, null),
                        default,
                        default)
                    .Result.Message)
                .Returns(_mocker.GetMock<IOperationResult<IPositionResponse>>().Object);

            _mocker
                .Setup<IGetUserProjectsInfoResponse, List<ProjectShortInfo>>(x => x.Projects)
                .Returns(_projects);
            _mocker
                .Setup<IOperationResult<IGetUserProjectsInfoResponse>, bool>(x => x.IsSuccess)
                .Returns(true);
            _mocker
                .Setup<IOperationResult<IGetUserProjectsInfoResponse>, IGetUserProjectsInfoResponse>(x => x.Body)
                .Returns(_mocker.GetMock<IGetUserProjectsInfoResponse>().Object);
            _mocker
                .Setup<IRequestClient<IGetUserProjectsInfoRequest>, IOperationResult<IGetUserProjectsInfoResponse>>(
                    x => x.GetResponse<IOperationResult<IGetUserProjectsInfoResponse>>(IGetUserProjectsInfoRequest.CreateObj(
                        _userId),
                        default,
                        default)
                    .Result.Message)
                .Returns(_mocker.GetMock<IOperationResult<IGetUserProjectsInfoResponse>>().Object);

            _mocker
                .Setup<IFileResponse, Guid>(x => x.Id)
                .Returns(Guid.NewGuid());
            _mocker
                .Setup<IOperationResult<IFileResponse>, bool>(x => x.IsSuccess)
                .Returns(true);
            _mocker
                .Setup<IOperationResult<IFileResponse>, IFileResponse>(x => x.Body)
                .Returns(_mocker.GetMock<IFileResponse>().Object);
            _mocker
                .Setup<IRequestClient<IGetFileRequest>, IOperationResult<IFileResponse>>(
                    x => x.GetResponse<IOperationResult<IFileResponse>>(IGetFileRequest.CreateObj(
                        _imageId, true),
                        default,
                        default)
                    .Result.Message)
                .Returns(_mocker.GetMock<IOperationResult<IFileResponse>>().Object);

            _loggerMock = new Mock<ILogger<GetUserCommand>>();

            _command = new GetUserCommand(
                _loggerMock.Object,
                _mocker.GetMock<IUserRepository>().Object,
                _mocker.GetMock<IUserResponseMapper>().Object,
                _mocker.GetMock<IRequestClient<IGetDepartmentRequest>>().Object,
                _mocker.GetMock<IRequestClient<IGetPositionRequest>>().Object,
                _mocker.GetMock<IRequestClient<IGetUserProjectsInfoRequest>>().Object,
                _mocker.GetMock<IRequestClient<IGetFileRequest>>().Object);
        }

        [Test]
        public void ThrowExсeptionWhenFilterIsNull()
        {
            _filter = null;

            Assert.Throws<BadRequestException>(() => _command.Execute(_filter));

            _mocker.Verify<IUserRepository, DbUser>(x => x.Get(It.IsAny<GetUserFilter>()), Times.Never());

            _mocker.Verify<IRequestClient<IGetDepartmentRequest>, IOperationResult<IGetDepartmentResponse>>(
                x => x.GetResponse<IOperationResult<IGetDepartmentResponse>>(
                    It.IsAny<object>(), default, default).Result.Message, Times.Never());

            _mocker.Verify<IRequestClient<IGetPositionRequest>, IOperationResult<IPositionResponse>>(
                x => x.GetResponse<IOperationResult<IPositionResponse>>(
                    It.IsAny<object>(), default, default).Result.Message, Times.Never());

            _mocker.Verify<IRequestClient<IGetUserProjectsInfoRequest>, IOperationResult<IGetUserProjectsInfoResponse>>(
                x => x.GetResponse<IOperationResult<IGetUserProjectsInfoResponse>>(
                    It.IsAny<object>(), default, default).Result.Message, Times.Never());

            _mocker.Verify<IRequestClient<IGetFileRequest>, IOperationResult<IFileResponse>>(
                x => x.GetResponse<IOperationResult<IFileResponse>>(
                    It.IsAny<object>(), default, default).Result.Message, Times.Never());

            _mocker.Verify<IUserResponseMapper, UserResponse>(x => x.Map(
                It.IsAny<DbUser>(),
                It.IsAny<DepartmentInfo>(),
                It.IsAny<PositionInfo>(),
                It.IsAny<List<ProjectInfo>>(),
                It.IsAny<List<ImageInfo>>(),
                It.IsAny<GetUserFilter>(),
                It.IsAny<List<string>>()),
                Times.Never);
        }

        [Test]
        public void ThrowExсeptionWhenFilterDoNotContainIdAndNameAndEmailData()
        {
            _filter.UserId = null;
            _filter.Name = null;
            _filter.Email = null;

            Assert.Throws<BadRequestException>(() => _command.Execute(_filter));

            _mocker.Verify<IUserRepository, DbUser>(x => x.Get(It.IsAny<GetUserFilter>()), Times.Never());

            _mocker.Verify<IRequestClient<IGetDepartmentRequest>, IOperationResult<IGetDepartmentResponse>>(
                x => x.GetResponse<IOperationResult<IGetDepartmentResponse>>(
                    It.IsAny<object>(), default, default).Result.Message, Times.Never());

            _mocker.Verify<IRequestClient<IGetPositionRequest>, IOperationResult<IPositionResponse>>(
                x => x.GetResponse<IOperationResult<IPositionResponse>>(
                    It.IsAny<object>(), default, default).Result.Message, Times.Never());

            _mocker.Verify<IRequestClient<IGetUserProjectsInfoRequest>, IOperationResult<IGetUserProjectsInfoResponse>>(
                x => x.GetResponse<IOperationResult<IGetUserProjectsInfoResponse>>(
                    It.IsAny<object>(), default, default).Result.Message, Times.Never());

            _mocker.Verify<IRequestClient<IGetFileRequest>, IOperationResult<IFileResponse>>(
                x => x.GetResponse<IOperationResult<IFileResponse>>(
                    It.IsAny<object>(), default, default).Result.Message, Times.Never());

            _mocker.Verify<IUserResponseMapper, UserResponse>(x => x.Map(
                It.IsAny<DbUser>(),
                It.IsAny<DepartmentInfo>(),
                It.IsAny<PositionInfo>(),
                It.IsAny<List<ProjectInfo>>(),
                It.IsAny<List<ImageInfo>>(),
                It.IsAny<GetUserFilter>(),
                It.IsAny<List<string>>()),
                Times.Never);
        }

        [Test]
        public void ThrowNotFoundExceptionWhenUserIsNotFound()
        {
            DbUser emptyDbUser = null;
            _mocker
                .Setup<IUserRepository, DbUser>(x => x.Get(It.IsAny<GetUserFilter>()))
                .Returns(emptyDbUser);

            Assert.Throws<NotFoundException>(() => _command.Execute(_filter));

            _mocker.Verify<IUserRepository, DbUser>(x => x.Get(It.IsAny<GetUserFilter>()), Times.Once());

            _mocker.Verify<IRequestClient<IGetDepartmentRequest>, IOperationResult<IGetDepartmentResponse>>(
                x => x.GetResponse<IOperationResult<IGetDepartmentResponse>>(
                    It.IsAny<object>(), default, default).Result.Message, Times.Never());

            _mocker.Verify<IRequestClient<IGetPositionRequest>, IOperationResult<IPositionResponse>>(
                x => x.GetResponse<IOperationResult<IPositionResponse>>(
                    It.IsAny<object>(), default, default).Result.Message, Times.Never());

            _mocker.Verify<IRequestClient<IGetUserProjectsInfoRequest>, IOperationResult<IGetUserProjectsInfoResponse>>(
                x => x.GetResponse<IOperationResult<IGetUserProjectsInfoResponse>>(
                    It.IsAny<object>(), default, default).Result.Message, Times.Never());

            _mocker.Verify<IRequestClient<IGetFileRequest>, IOperationResult<IFileResponse>>(
                x => x.GetResponse<IOperationResult<IFileResponse>>(
                    It.IsAny<object>(), default, default).Result.Message, Times.Never());

            _mocker.Verify<IUserResponseMapper, UserResponse>(x => x.Map(
                It.IsAny<DbUser>(),
                It.IsAny<DepartmentInfo>(),
                It.IsAny<PositionInfo>(),
                It.IsAny<List<ProjectInfo>>(),
                It.IsAny<List<ImageInfo>>(),
                It.IsAny<GetUserFilter>(),
                It.IsAny<List<string>>()),
                Times.Never);
        }

        [Test]
        public void ReturnSuccessfulResponse()
        {
            SerializerAssert.AreEqual(_userResponse, _command.Execute(_filter));
            _mocker.Verify<IUserRepository, DbUser>(x => x.Get(It.IsAny<GetUserFilter>()), Times.Once());

            _mocker.Verify<IRequestClient<IGetDepartmentRequest>, IOperationResult<IGetDepartmentResponse>>(
                x => x.GetResponse<IOperationResult<IGetDepartmentResponse>>(
                    It.IsAny<object>(), default, default).Result.Message, Times.Never());

            _mocker.Verify<IRequestClient<IGetPositionRequest>, IOperationResult<IPositionResponse>>(
                x => x.GetResponse<IOperationResult<IPositionResponse>>(
                    It.IsAny<object>(), default, default).Result.Message, Times.Once());

            _mocker.Verify<IRequestClient<IGetUserProjectsInfoRequest>, IOperationResult<IGetUserProjectsInfoResponse>>(
                x => x.GetResponse<IOperationResult<IGetUserProjectsInfoResponse>>(
                    It.IsAny<object>(), default, default).Result.Message, Times.Once());

            _mocker.Verify<IRequestClient<IGetFileRequest>, IOperationResult<IFileResponse>>(
                x => x.GetResponse<IOperationResult<IFileResponse>>(
                    It.IsAny<object>(), default, default).Result.Message, Times.Once());

            _mocker.Verify<IUserResponseMapper, UserResponse>(x => x.Map(
                It.IsAny<DbUser>(),
                It.IsAny<DepartmentInfo>(),
                It.IsAny<PositionInfo>(),
                It.IsAny<List<ProjectInfo>>(),
                It.IsAny<List<ImageInfo>>(),
                It.IsAny<GetUserFilter>(),
                It.IsAny<List<string>>()),
                Times.Once);
        }
    }
}