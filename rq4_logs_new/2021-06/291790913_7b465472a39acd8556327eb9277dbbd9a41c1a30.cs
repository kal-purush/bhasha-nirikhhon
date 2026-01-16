using LT.DigitalOffice.Kernel.Broker;
using LT.DigitalOffice.Models.Broker.Requests.Company;
using LT.DigitalOffice.Models.Broker.Responses.Company;
using LT.DigitalOffice.UnitTestKernel;
using LT.DigitalOffice.UserService.Business.Interfaces;
using LT.DigitalOffice.UserService.Data.Interfaces;
using LT.DigitalOffice.UserService.Mappers.Models.Interfaces;
using LT.DigitalOffice.UserService.Models.Db;
using LT.DigitalOffice.UserService.Models.Dto.Enums;
using LT.DigitalOffice.UserService.Models.Dto.Models;
using LT.DigitalOffice.UserService.Models.Dto.Responses.User;
using MassTransit;
using Moq;
using Moq.AutoMock;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace LT.DigitalOffice.UserService.Business.UnitTests
{
    class FindUserCommandTests
    {
        private int _skipCount;
        private int _takeCount;
        private Guid _departmentId;

        private List<DbUser> _dbUsers;
        private List<UserInfo> _usersInfo;
        private AutoMocker _mocker;
        private IFindUserCommand _command;

        private Mock<Response<IOperationResult<IFindDepartmentUsersResponse>>> _operationResultBroker;

        [OneTimeSetUp]
        public void OneTimeSetUp()
        {

            _departmentId = Guid.NewGuid();
            _skipCount = 0;

            _mocker = new AutoMocker();
            _command = _mocker.CreateInstance<FindUserCommand>();
            List<Guid> userIds = new() { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

            _dbUsers = new List<DbUser>
            {
                new DbUser
                {
                    Id = userIds[0],
                    FirstName = "Ivan",
                    LastName = "Ivanov",
                    MiddleName = "Ivanovich",
                    Status = (int)UserStatus.Vacation,
                    AvatarFileId = Guid.NewGuid(),
                    IsAdmin = false
                },
                new DbUser
                {
                    Id = userIds[1],
                    FirstName = "Ivan",
                    LastName = "Ivanov",
                    MiddleName = "Ivanovich",
                    Status = (int)UserStatus.Vacation,
                    AvatarFileId = Guid.NewGuid(),
                    IsAdmin = false
                },
                new DbUser
                {
                    Id = userIds[2],
                    FirstName = "Ivan",
                    LastName = "Ivanov",
                    MiddleName = "Ivanovich",
                    Status = (int)UserStatus.Vacation,
                    AvatarFileId = Guid.NewGuid(),
                    IsAdmin = false
                }
            };

            _usersInfo = new();
            foreach (DbUser dbUser in _dbUsers)
            {
                _usersInfo.Add(
                    new UserInfo
                    {
                        Id = dbUser.Id,
                        FirstName = dbUser.FirstName,
                        LastName = dbUser.LastName,
                        MiddleName = dbUser.MiddleName,
                        IsAdmin = dbUser.IsAdmin,
                        Status = (UserStatus)dbUser.Status
                    });
            }

            _takeCount = _usersInfo.Count;

            var usersResponse = new Mock<IFindDepartmentUsersResponse>();
            usersResponse.Setup(x => x.UserIds).Returns(userIds);
            usersResponse.Setup(x => x.TotalCount).Returns(_usersInfo.Count);

            _operationResultBroker = new Mock<Response<IOperationResult<IFindDepartmentUsersResponse>>>();
            _operationResultBroker.Setup(x => x.Message.Body).Returns(usersResponse.Object);
            _operationResultBroker.Setup(x => x.Message.IsSuccess).Returns(true);
            _operationResultBroker.Setup(x => x.Message.Errors).Returns(new List<string> { "Some errors" });
        }

        [SetUp]
        public void SetUp()
        {
            _mocker.GetMock<IUserRepository>().Reset();
            _mocker.GetMock<IUserInfoMapper>().Reset();
            _mocker.GetMock<IRequestClient<IFindDepartmentUsersRequest>>().Reset();

            _mocker
               .Setup<IRequestClient<IFindDepartmentUsersRequest>, Task<Response<IOperationResult<IFindDepartmentUsersResponse>>>>(
               x => x.GetResponse<IOperationResult<IFindDepartmentUsersResponse>>(
                   IFindDepartmentUsersRequest.CreateObj(_departmentId, _skipCount, _takeCount), default, TimeSpan.FromSeconds(2)))
               .Returns(Task.FromResult(_operationResultBroker.Object));
        }

        [Test]
        public void ShouldEmptyListUsersWhenRequestClientThrowException()
        {
            UsersResponse result = new()
            {
                Users = new List<UserInfo>(),
                TotalCount = 0,
                Errors = new() { $"Can not get department users with department id {_departmentId}. Please try again later." }
            };

            _operationResultBroker.Setup(x => x.Message.IsSuccess).Returns(false);

            _mocker
               .Setup<IRequestClient<IFindDepartmentUsersRequest>, Task<Response<IOperationResult<IFindDepartmentUsersResponse>>>>(
               x => x.GetResponse<IOperationResult<IFindDepartmentUsersResponse>>(
                   IFindDepartmentUsersRequest.CreateObj(_departmentId, _skipCount, _takeCount), default, TimeSpan.FromSeconds(2)))
               .Throws(new Exception());

            SerializerAssert.AreEqual(result, _command.Execute(_skipCount, _takeCount, _departmentId));

            _mocker.Verify<IRequestClient<IFindDepartmentUsersRequest>, Task<Response<IOperationResult<IFindDepartmentUsersResponse>>>>(
                x => x.GetResponse<IOperationResult<IFindDepartmentUsersResponse>>(
                    IFindDepartmentUsersRequest.CreateObj(_departmentId, _skipCount, _takeCount), default, TimeSpan.FromSeconds(2)), Times.Once);
            _mocker.Verify<IUserRepository, IEnumerable<DbUser>>(x => x.Get(It.IsAny<List<Guid>>()), Times.Never);
            _mocker.Verify<IUserInfoMapper, UserInfo>(x => x.Map(It.IsAny<DbUser>()), Times.Never);
        }

        [Test]
        public void ShouldEmptyListUsersWhenBrokerResponseIsNotSuccess()
        {
            UsersResponse result = new()
            {
                Users = new List<UserInfo>(),
                TotalCount = 0
            };

            _operationResultBroker.Setup(x => x.Message.IsSuccess).Returns(false);

            SerializerAssert.AreEqual(result, _command.Execute(_skipCount, _takeCount, _departmentId));

            _mocker.Verify<IRequestClient<IFindDepartmentUsersRequest>, Task<Response<IOperationResult<IFindDepartmentUsersResponse>>>>(
                x => x.GetResponse<IOperationResult<IFindDepartmentUsersResponse>>(
                    IFindDepartmentUsersRequest.CreateObj(_departmentId, _skipCount, _takeCount), default, TimeSpan.FromSeconds(2)), Times.Once);
            _mocker.Verify<IUserRepository, IEnumerable<DbUser>>(x => x.Get(It.IsAny<List<Guid>>()), Times.Never);
            _mocker.Verify<IUserInfoMapper, UserInfo>(x => x.Map(It.IsAny<DbUser>()), Times.Never);
        }

        [Test]
        public void ShouldReturnUsersWithoutDepartmentId()
        {
            int totalCount = _usersInfo.Count;
            UsersResponse result = new()
            {
                Users = _usersInfo,
                TotalCount = _usersInfo.Count
            };

            _mocker
                .Setup<IUserRepository, IEnumerable<DbUser>>(x => x.Find(_skipCount, _takeCount, out totalCount))
                .Returns(_dbUsers);

            _mocker
                .SetupSequence<IUserInfoMapper, UserInfo>(x => x.Map(It.IsAny<DbUser>()))
                .Returns(_usersInfo[0])
                .Returns(_usersInfo[1])
                .Returns(_usersInfo[2]);

            SerializerAssert.AreEqual(result, _command.Execute(_skipCount, _takeCount, null));

            _mocker.Verify<IRequestClient<IFindDepartmentUsersRequest>, Task<Response<IOperationResult<IFindDepartmentUsersResponse>>>>(
               x => x.GetResponse<IOperationResult<IFindDepartmentUsersResponse>>(
                   IFindDepartmentUsersRequest.CreateObj(_departmentId, _skipCount, _takeCount), default, TimeSpan.FromSeconds(2)), Times.Never);
            _mocker.Verify<IUserRepository, IEnumerable<DbUser>>(x => x.Find(_skipCount, _takeCount, out totalCount), Times.Once);
            _mocker.Verify<IUserInfoMapper, UserInfo>(x => x.Map(It.IsAny<DbUser>()), Times.Exactly(_usersInfo.Count));
        }

        [Test]
        public void ShouldReturnUsersByDepartmentId()
        {
            UsersResponse result = new()
            {
                Users = _usersInfo,
                TotalCount = _usersInfo.Count
            };

            _operationResultBroker.Setup(x => x.Message.IsSuccess).Returns(true);

            _mocker
                .Setup<IUserRepository, IEnumerable<DbUser>>(x => x.Get(It.IsAny<List<Guid>>()))
                .Returns(_dbUsers);

            _mocker
                .SetupSequence<IUserInfoMapper, UserInfo>(x => x.Map(It.IsAny<DbUser>()))
                .Returns(_usersInfo[0])
                .Returns(_usersInfo[1])
                .Returns(_usersInfo[2]);

            SerializerAssert.AreEqual(result, _command.Execute(_skipCount, _takeCount, _departmentId));

            _mocker.Verify<IRequestClient<IFindDepartmentUsersRequest>, Task<Response<IOperationResult<IFindDepartmentUsersResponse>>>>(
               x => x.GetResponse<IOperationResult<IFindDepartmentUsersResponse>>(
                   IFindDepartmentUsersRequest.CreateObj(_departmentId, _skipCount, _takeCount), default, TimeSpan.FromSeconds(2)), Times.Once);
            _mocker.Verify<IUserRepository, IEnumerable<DbUser>>(x => x.Get(It.IsAny<List<Guid>>()), Times.Once);
            _mocker.Verify<IUserInfoMapper, UserInfo>(x => x.Map(It.IsAny<DbUser>()), Times.Exactly(_usersInfo.Count));
        }
    }
}