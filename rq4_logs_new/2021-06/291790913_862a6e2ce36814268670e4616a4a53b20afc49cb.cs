using LT.DigitalOffice.Kernel.Broker;
using LT.DigitalOffice.Kernel.Exceptions.Models;
using LT.DigitalOffice.Models.Broker.Requests.Token;
using LT.DigitalOffice.UnitTestKernel;
using LT.DigitalOffice.UserService.Business.Commands.Credentials;
using LT.DigitalOffice.UserService.Business.Commands.Credentials.Interfaces;
using LT.DigitalOffice.UserService.Data.Interfaces;
using LT.DigitalOffice.UserService.Mappers.Db.Interfaces;
using LT.DigitalOffice.UserService.Models.Db;
using LT.DigitalOffice.UserService.Models.Dto.Requests.Credentials;
using LT.DigitalOffice.UserService.Models.Dto.Responses.Credentials;
using MassTransit;
using Microsoft.Extensions.Logging;
using Moq;
using Moq.AutoMock;
using NUnit.Framework;
using System;

namespace LT.DigitalOffice.UserService.Business.UnitTests
{
    class CreateCredentialsCommandTests
    {
        private Mock<ILogger<CreateCredentialsCommand>> _loggerMock;
        private AutoMocker _mocker;
        private ICreateCredentialsCommand _command;

        private CreateCredentialsRequest _request;
        private DbPendingUser _dbPendingUser;
        private string _userToken;
        private Guid _userId = Guid.NewGuid();
        private string _password = "password";
        private CredentialsResponse _responce;

        [SetUp]
        public void SetUp()
        {
            _dbPendingUser = new()
            {
                UserId = _userId,
                Password = _password
            };

            _request = new CreateCredentialsRequest()
            {
                UserId = _userId,
                Login = "login",
                Password = _password
            };

            _userToken = "token";

            _responce = new()
            {
                UserId = _userId,
                Token = _userToken
            };

            _loggerMock = new Mock<ILogger<CreateCredentialsCommand>>();

            _mocker = new AutoMocker();

            _mocker
                .Setup<IUserRepository, DbPendingUser>(
                    x => x.GetPendingUser(_userId))
                .Returns(_dbPendingUser);

            _mocker
                .Setup<IUserRepository>(
                    x => x.DeletePendingUser(It.IsAny<Guid>()));

            _mocker
                .Setup<IUserRepository, bool>(
                    x => x.SwitchActiveStatus(It.IsAny<Guid>(), true))
                .Returns(true);

            _mocker
                .Setup<IDbUserCredentialsMapper, DbUserCredentials>(
                    x => x.Map(It.IsAny<CreateCredentialsRequest>(),
                        It.IsAny<string>(),
                        It.IsAny<string>()))
                .Returns(new DbUserCredentials());

            _mocker
                .Setup<IUserCredentialsRepository>(
                    x => x.CheckLogin(It.IsAny<string>(), It.IsAny<Guid>()));

            _mocker
                .Setup<IUserCredentialsRepository, Guid>(
                    x => x.Create(It.IsAny<DbUserCredentials>()))
                .Returns(new Guid());

            _mocker
                .Setup<IOperationResult<string>, string>(x => x.Body)
                .Returns(_userToken);
            _mocker
                .Setup<IOperationResult<string>, bool>(x => x.IsSuccess)
                .Returns(true);
            _mocker
                .Setup<IRequestClient<IGetTokenRequest>, IOperationResult<string>>(
                    x => x.GetResponse<IOperationResult<string>>(
                        IGetTokenRequest.CreateObj(_userId),
                        default,
                        default)
                    .Result.Message)
                .Returns(_mocker.GetMock<IOperationResult<string>>().Object);

            _command = new CreateCredentialsCommand(
                _mocker.GetMock<IDbUserCredentialsMapper>().Object,
                _mocker.GetMock<IUserRepository>().Object,
                _mocker.GetMock<IUserCredentialsRepository>().Object,
                _mocker.GetMock<IRequestClient<IGetTokenRequest>>().Object,
                _loggerMock.Object);
        }

        [Test]
        public void ThrowExсeptionWhenRequestIsNull()
        {
            _request = null;

            Assert.Throws<BadRequestException>(() => _command.Execute(_request));

            _mocker.Verify<IUserRepository, DbPendingUser>(
                x => x.GetPendingUser(It.IsAny<Guid>()),
                Times.Never());

            _mocker.Verify<IUserCredentialsRepository>(
                x => x.CheckLogin(It.IsAny<string>(), It.IsAny<Guid>()),
                Times.Never());

            _mocker.Verify<IDbUserCredentialsMapper, DbUserCredentials>(
                x => x.Map(It.IsAny<CreateCredentialsRequest>(),
                    It.IsAny<string>(),
                    It.IsAny<string>()),
                Times.Never());

            _mocker.Verify<IRequestClient<IGetTokenRequest>, IOperationResult<string>>(
                x => x.GetResponse<IOperationResult<string>>(
                    IGetTokenRequest.CreateObj(_userId),
                    default,
                    default)
                .Result.Message,
                Times.Never());

            _mocker.Verify<IUserCredentialsRepository, Guid>(
                x => x.Create(It.IsAny<DbUserCredentials>()),
                Times.Never());

            _mocker.Verify<IUserRepository>(
                x => x.DeletePendingUser(It.IsAny<Guid>()),
                Times.Never());

            _mocker.Verify<IUserRepository, bool>(
                x => x.SwitchActiveStatus(It.IsAny<Guid>(), It.IsAny<bool>()),
                Times.Never());
        }

        [Test]
        public void ThrowExсeptionWhenDbPendingUserIsNull()
        {
            DbPendingUser dbPendingUser = null;
            _mocker
                .Setup<IUserRepository, DbPendingUser>(
                    x => x.GetPendingUser(_userId))
                .Returns(dbPendingUser);

            Assert.Throws<NotFoundException>(() => _command.Execute(_request));

            _mocker.Verify<IUserRepository, DbPendingUser>(
                x => x.GetPendingUser(It.IsAny<Guid>()),
                Times.Once());

            _mocker.Verify<IUserCredentialsRepository>(
                x => x.CheckLogin(It.IsAny<string>(), It.IsAny<Guid>()),
                Times.Never());

            _mocker.Verify<IDbUserCredentialsMapper, DbUserCredentials>(
                x => x.Map(It.IsAny<CreateCredentialsRequest>(),
                    It.IsAny<string>(),
                    It.IsAny<string>()),
                Times.Never());

            _mocker.Verify<IRequestClient<IGetTokenRequest>, IOperationResult<string>>(
                x => x.GetResponse<IOperationResult<string>>(
                    IGetTokenRequest.CreateObj(_userId),
                    default,
                    default)
                .Result.Message,
                Times.Never());

            _mocker.Verify<IUserCredentialsRepository, Guid>(
                x => x.Create(It.IsAny<DbUserCredentials>()),
                Times.Never());

            _mocker.Verify<IUserRepository>(
                x => x.DeletePendingUser(It.IsAny<Guid>()),
                Times.Never());

            _mocker.Verify<IUserRepository, bool>(
                x => x.SwitchActiveStatus(It.IsAny<Guid>(), It.IsAny<bool>()),
                Times.Never());
        }

        [Test]
        public void ThrowExсeptionWhenLoginIsBusyOrCredentialsIsExist()
        {
            _mocker
                .Setup<IUserCredentialsRepository>(
                    x => x.CheckLogin(It.IsAny<string>(), It.IsAny<Guid>()))
                .Throws(new BadRequestException());

            Assert.Throws<BadRequestException>(() => _command.Execute(_request));

            _mocker.Verify<IUserRepository, DbPendingUser>(
                x => x.GetPendingUser(It.IsAny<Guid>()),
                Times.Once());

            _mocker.Verify<IUserCredentialsRepository>(
                x => x.CheckLogin(It.IsAny<string>(), It.IsAny<Guid>()),
                Times.Once());

            _mocker.Verify<IDbUserCredentialsMapper, DbUserCredentials>(
                x => x.Map(It.IsAny<CreateCredentialsRequest>(),
                    It.IsAny<string>(),
                    It.IsAny<string>()),
                Times.Never());

            _mocker.Verify<IRequestClient<IGetTokenRequest>, IOperationResult<string>>(
                x => x.GetResponse<IOperationResult<string>>(
                    IGetTokenRequest.CreateObj(_userId),
                    default,
                    default)
                .Result.Message,
                Times.Never());

            _mocker.Verify<IUserCredentialsRepository, Guid>(
                x => x.Create(It.IsAny<DbUserCredentials>()),
                Times.Never());

            _mocker.Verify<IUserRepository>(
                x => x.DeletePendingUser(It.IsAny<Guid>()),
                Times.Never());

            _mocker.Verify<IUserRepository, bool>(
                x => x.SwitchActiveStatus(It.IsAny<Guid>(), It.IsAny<bool>()),
                Times.Never());
        }

        [Test]
        public void ThrowExсeptionWhenPasswordIsNotRight()
        {
            _request.Password = "notRightPassword";

            Assert.Throws<ForbiddenException>(() => _command.Execute(_request));

            _mocker.Verify<IUserRepository, DbPendingUser>(
                x => x.GetPendingUser(It.IsAny<Guid>()),
                Times.Once());

            _mocker.Verify<IUserCredentialsRepository>(
                x => x.CheckLogin(It.IsAny<string>(), It.IsAny<Guid>()),
                Times.Once());

            _mocker.Verify<IDbUserCredentialsMapper, DbUserCredentials>(
                x => x.Map(It.IsAny<CreateCredentialsRequest>(),
                    It.IsAny<string>(),
                    It.IsAny<string>()),
                Times.Never());

            _mocker.Verify<IRequestClient<IGetTokenRequest>, IOperationResult<string>>(
                x => x.GetResponse<IOperationResult<string>>(
                    IGetTokenRequest.CreateObj(_userId),
                    default,
                    default)
                .Result.Message,
                Times.Never());

            _mocker.Verify<IUserCredentialsRepository, Guid>(
                x => x.Create(It.IsAny<DbUserCredentials>()),
                Times.Never());

            _mocker.Verify<IUserRepository>(
                x => x.DeletePendingUser(It.IsAny<Guid>()),
                Times.Never());

            _mocker.Verify<IUserRepository, bool>(
                x => x.SwitchActiveStatus(It.IsAny<Guid>(), It.IsAny<bool>()),
                Times.Never());
        }

        [Test]
        public void ThrowExсeptionWhenBrokerResponceIsNotSuccess()
        {
            _mocker
                .Setup<IOperationResult<string>, bool>(x => x.IsSuccess)
                .Returns(false);

            Assert.Throws<BadRequestException>(() => _command.Execute(_request));

            _mocker.Verify<IUserRepository, DbPendingUser>(
                x => x.GetPendingUser(It.IsAny<Guid>()),
                Times.Once());

            _mocker.Verify<IUserCredentialsRepository>(
                x => x.CheckLogin(It.IsAny<string>(), It.IsAny<Guid>()),
                Times.Once());

            _mocker.Verify<IDbUserCredentialsMapper, DbUserCredentials>(
                x => x.Map(It.IsAny<CreateCredentialsRequest>(),
                    It.IsAny<string>(),
                    It.IsAny<string>()),
                Times.Never());

            _mocker.Verify<IRequestClient<IGetTokenRequest>, IOperationResult<string>>(
                x => x.GetResponse<IOperationResult<string>>(
                    IGetTokenRequest.CreateObj(_userId),
                    default,
                    default)
                .Result.Message,
                Times.Once());

            _mocker.Verify<IUserCredentialsRepository, Guid>(
                x => x.Create(It.IsAny<DbUserCredentials>()),
                Times.Never());

            _mocker.Verify<IUserRepository>(
                x => x.DeletePendingUser(It.IsAny<Guid>()),
                Times.Never());

            _mocker.Verify<IUserRepository, bool>(
                x => x.SwitchActiveStatus(It.IsAny<Guid>(), It.IsAny<bool>()),
                Times.Never());
        }
        [Test]
        public void ThrowExсeptionWhenMapperThrowsIt()
        {
            _mocker
                .Setup<IDbUserCredentialsMapper, DbUserCredentials>(
                    x => x.Map(It.IsAny<CreateCredentialsRequest>(),
                        It.IsAny<string>(),
                        It.IsAny<string>()))
                .Throws(new ArgumentNullException());

            Assert.Throws<BadRequestException>(() => _command.Execute(_request));

            _mocker.Verify<IUserRepository, DbPendingUser>(
                x => x.GetPendingUser(It.IsAny<Guid>()),
                Times.Once());

            _mocker.Verify<IUserCredentialsRepository>(
                x => x.CheckLogin(It.IsAny<string>(), It.IsAny<Guid>()),
                Times.Once());

            _mocker.Verify<IRequestClient<IGetTokenRequest>, IOperationResult<string>>(
                x => x.GetResponse<IOperationResult<string>>(
                    IGetTokenRequest.CreateObj(_userId),
                    default,
                    default)
                .Result.Message,
                Times.Once());

            _mocker.Verify<IDbUserCredentialsMapper, DbUserCredentials>(
                x => x.Map(It.IsAny<CreateCredentialsRequest>(),
                    It.IsAny<string>(),
                    It.IsAny<string>()),
                Times.Once());

            _mocker.Verify<IUserCredentialsRepository, Guid>(
                x => x.Create(It.IsAny<DbUserCredentials>()),
                Times.Never());

            _mocker.Verify<IUserRepository>(
                x => x.DeletePendingUser(It.IsAny<Guid>()),
                Times.Never());

            _mocker.Verify<IUserRepository, bool>(
                x => x.SwitchActiveStatus(It.IsAny<Guid>(), It.IsAny<bool>()),
                Times.Never());
        }

        [Test]
        public void ThrowExсeptionWhenUserCredentialsRepositoryThrowsIt()
        {
            _mocker
                .Setup<IUserCredentialsRepository, Guid>(
                    x => x.Create(It.IsAny<DbUserCredentials>()))
                .Throws(new Exception());

            Assert.Throws<BadRequestException>(() => _command.Execute(_request));

            _mocker.Verify<IUserRepository, DbPendingUser>(
                x => x.GetPendingUser(It.IsAny<Guid>()),
                Times.Once());

            _mocker.Verify<IUserCredentialsRepository>(
                x => x.CheckLogin(It.IsAny<string>(), It.IsAny<Guid>()),
                Times.Once());

            _mocker.Verify<IRequestClient<IGetTokenRequest>, IOperationResult<string>>(
                x => x.GetResponse<IOperationResult<string>>(
                    IGetTokenRequest.CreateObj(_userId),
                    default,
                    default)
                .Result.Message,
                Times.Once());

            _mocker.Verify<IDbUserCredentialsMapper, DbUserCredentials>(
                x => x.Map(It.IsAny<CreateCredentialsRequest>(),
                    It.IsAny<string>(),
                    It.IsAny<string>()),
                Times.Once());

            _mocker.Verify<IUserCredentialsRepository, Guid>(
                x => x.Create(It.IsAny<DbUserCredentials>()),
                Times.Once());

            _mocker.Verify<IUserRepository>(
                x => x.DeletePendingUser(It.IsAny<Guid>()),
                Times.Never());

            _mocker.Verify<IUserRepository, bool>(
                x => x.SwitchActiveStatus(It.IsAny<Guid>(), It.IsAny<bool>()),
                Times.Never());
        }

        [Test]
        public void ThrowExсeptionWhenUserRepositoryThrowsItWhenDeletePendingUser()
        {
            _mocker
                .Setup<IUserRepository>(
                    x => x.DeletePendingUser(It.IsAny<Guid>()))
                .Throws(new Exception());

            Assert.Throws<BadRequestException>(() => _command.Execute(_request));

            _mocker.Verify<IUserRepository, DbPendingUser>(
                x => x.GetPendingUser(It.IsAny<Guid>()),
                Times.Once());

            _mocker.Verify<IUserCredentialsRepository>(
                x => x.CheckLogin(It.IsAny<string>(), It.IsAny<Guid>()),
                Times.Once());

            _mocker.Verify<IRequestClient<IGetTokenRequest>, IOperationResult<string>>(
                x => x.GetResponse<IOperationResult<string>>(
                    IGetTokenRequest.CreateObj(_userId),
                    default,
                    default)
                .Result.Message,
                Times.Once());

            _mocker.Verify<IDbUserCredentialsMapper, DbUserCredentials>(
                x => x.Map(It.IsAny<CreateCredentialsRequest>(),
                    It.IsAny<string>(),
                    It.IsAny<string>()),
                Times.Once());

            _mocker.Verify<IUserCredentialsRepository, Guid>(
                x => x.Create(It.IsAny<DbUserCredentials>()),
                Times.Once());

            _mocker.Verify<IUserRepository>(
                x => x.DeletePendingUser(It.IsAny<Guid>()),
                Times.Once());

            _mocker.Verify<IUserRepository, bool>(
                x => x.SwitchActiveStatus(It.IsAny<Guid>(), It.IsAny<bool>()),
                Times.Never());
        }

        [Test]
        public void ThrowExсeptionWhenUserRepositoryThrowsItWhenSwitchActiveStatus()
        {
            _mocker
                .Setup<IUserRepository, bool>(
                    x => x.SwitchActiveStatus(It.IsAny<Guid>(), true))
                .Throws(new Exception());

            Assert.Throws<BadRequestException>(() => _command.Execute(_request));

            _mocker.Verify<IUserRepository, DbPendingUser>(
                x => x.GetPendingUser(It.IsAny<Guid>()),
                Times.Once());

            _mocker.Verify<IUserCredentialsRepository>(
                x => x.CheckLogin(It.IsAny<string>(), It.IsAny<Guid>()),
                Times.Once());

            _mocker.Verify<IRequestClient<IGetTokenRequest>, IOperationResult<string>>(
                x => x.GetResponse<IOperationResult<string>>(
                    IGetTokenRequest.CreateObj(_userId),
                    default,
                    default)
                .Result.Message,
                Times.Once());

            _mocker.Verify<IDbUserCredentialsMapper, DbUserCredentials>(
                x => x.Map(It.IsAny<CreateCredentialsRequest>(),
                    It.IsAny<string>(),
                    It.IsAny<string>()),
                Times.Once());

            _mocker.Verify<IUserCredentialsRepository, Guid>(
                x => x.Create(It.IsAny<DbUserCredentials>()),
                Times.Once());

            _mocker.Verify<IUserRepository>(
                x => x.DeletePendingUser(It.IsAny<Guid>()),
                Times.Once());

            _mocker.Verify<IUserRepository, bool>(
                x => x.SwitchActiveStatus(It.IsAny<Guid>(), It.IsAny<bool>()),
                Times.Once());
        }

        [Test]
        public void SuccessTest()
        {
            SerializerAssert.AreEqual(_responce, _command.Execute(_request));

            _mocker.Verify<IUserRepository, DbPendingUser>(
                x => x.GetPendingUser(It.IsAny<Guid>()),
                Times.Once());

            _mocker.Verify<IUserCredentialsRepository>(
                x => x.CheckLogin(It.IsAny<string>(), It.IsAny<Guid>()),
                Times.Once());

            _mocker.Verify<IRequestClient<IGetTokenRequest>, IOperationResult<string>>(
                x => x.GetResponse<IOperationResult<string>>(
                    IGetTokenRequest.CreateObj(_userId),
                    default,
                    default)
                .Result.Message,
                Times.Once());

            _mocker.Verify<IDbUserCredentialsMapper, DbUserCredentials>(
                x => x.Map(It.IsAny<CreateCredentialsRequest>(),
                    It.IsAny<string>(),
                    It.IsAny<string>()),
                Times.Once());

            _mocker.Verify<IUserCredentialsRepository, Guid>(
                x => x.Create(It.IsAny<DbUserCredentials>()),
                Times.Once());

            _mocker.Verify<IUserRepository>(
                x => x.DeletePendingUser(It.IsAny<Guid>()),
                Times.Once());

            _mocker.Verify<IUserRepository, bool>(
                x => x.SwitchActiveStatus(It.IsAny<Guid>(), It.IsAny<bool>()),
                Times.Once());
        }
    }
}