using Microsoft.AspNetCore.Identity;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using NUnit.Framework;
using ShareCar.Db.Entities;
using ShareCar.Db.Repositories.User_Repository;
using ShareCar.Dto.Identity;
using ShareCar.Dto.Identity.Google;
using ShareCar.Logic.Identity_Logic;
using ShareCar.Logic.User_Logic;
using System;
using System.Collections.Generic;
using System.Security.Claims;
using System.Threading.Tasks;

namespace Tests
{
    public class GoogleIdentityTest
    {

        //  Mock<UserManager<User>> userManager = new Mock<UserManager<User>(new Mock<IUserStore <User>>().Object, new Mock<IOptions <IdentityOptions>>().Object, new Mock<IPasswordHasher<User>>().Object, new Mock<IEnumerable <IUserValidator<User>>>().Object, new Mock<IEnumerable <IPasswordValidator<User>>>().Object, new Mock<ILookupNormalizer>().Object, new Mock<IdentityErrorDescriber>().Object, new Mock<IServiceProvider>().Object, new Mock<ILogger<UserManager<User>>>().Object)>();

        //  Mock<UserManager<User>> userManager = new Mock<UserManager<User>>(MockBehavior.Strict, new object[] );
        Mock<IUserRepository> userRepository = new Mock<IUserRepository>();
        Mock<IJwtFactory> jwtFactory = new Mock<IJwtFactory>();
        GoogleUserDataDto loginData = new GoogleUserDataDto
        {
            Id = 1,
            Email = "test@test.com",
            GivenName = "Test",
            FamilyName = "Test",
            ImageUrl = "..."
        };

    [SetUp]
        public void Setup()
        {

        }

        [Test]
        public async Task Login_NewUser_ReturnsNull()
        {

            User user = null;

            userRepository.Setup(x => x.GetUserByEmail(ShareCar.Dto.EmailType.GOOGLE, "test@test.com")).Returns(user);

           var googleIdentity = new GoogleIdentity(jwtFactory.Object, userRepository.Object);

            Assert.IsNull(await googleIdentity.Login(loginData));

        }

        [Test]
        public async Task Login_ExistingUnverifiedUser_ReturnsNull()
        {

            User user = new User {
                GoogleVerified = false
            };

            userRepository.Setup(x => x.GetUserByEmail(ShareCar.Dto.EmailType.GOOGLE, "test@test.com")).Returns(user);

            var googleIdentity = new GoogleIdentity(jwtFactory.Object, userRepository.Object);

            Assert.IsNull(await googleIdentity.Login(loginData));
        }

        [Test]
        public async Task Login_VerifiedUser_ReturnsString()
        {
            User user = new User
            {
                Id = "1",
                UserName = loginData.Email,
                Email = loginData.Email,
                GoogleVerified = true
            };

            userRepository.Setup(x => x.GetUserByEmail(ShareCar.Dto.EmailType.GOOGLE, loginData.Email)).Returns(user);
            userRepository.Setup(x => x.GetUserByEmail(ShareCar.Dto.EmailType.LOGIN, loginData.Email)).Returns(user);
            jwtFactory.Setup(x => x.GenerateEncodedToken(loginData.Email, It.IsAny<ClaimsIdentity>())).Returns(Task.FromResult("string"));

            var googleIdentity = new GoogleIdentity(jwtFactory.Object, userRepository.Object);

            Assert.NotNull(await googleIdentity.Login(loginData));
        }
    }
}