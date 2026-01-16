using Microsoft.AspNetCore.Http;
using Moq;
using Nt.Domain.Entities.User;
using Nt.Domain.ServiceContracts.User;
using Nt.Infrastructure.WebApi.Controllers;
using Nt.Infrastructure.WebApi.ViewModels.Areas.User.ChangePassword;
using Nt.Infrastructure.WebApi.ViewModels.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Nt.Infrastructure.Tests.Controllers.UserControllerTests
{
    public  class ChangePasswordTests : ControllerTestBase<UserProfileEntity>
    {
        public ChangePasswordTests(ITestOutputHelper testOutput) : base(testOutput)
        {

        }

        [Theory]
        [MemberData(nameof(ChangePasswordTestErrorTestData))]
        public async Task ChangePasswordTestError(ChangePasswordRequest request, params string[] errorMessage)
        {

            var userProfileEntity = Mapper.Map<UserProfileEntity>(request);
            var mockUserProfileService = new Mock<IUserProfileService>();
            mockUserProfileService.Setup(x => x.UpdateUserAsync(It.IsAny<UserProfileEntity>()))
                .Returns(Task.FromResult(true));

            var userController = new UserController(Mapper, mockUserProfileService.Object, null, null);
            MockModelState(request, userController);

            var result = await userController.ChangePassword(request);
            foreach (var err in errorMessage)
            {
                Assert.Contains(err, result.ErrorMessage.Split(Environment.NewLine));
            }
        }

        public static IEnumerable<object[]> ChangePasswordTestErrorTestData => new[]
        {
            new object[]{ new ChangePasswordRequest { OldPassword = "1" },"Password should contain minimum of 8 characters"},
            new object[]{new ChangePasswordRequest { OldPassword = "Sample", NewPassword = "SampleNew" }, "Password should contain minimum of 8 characters" }
        };


        [Theory]
        [MemberData(nameof(ChangePasswordTestSuccessTestData))]
        public async Task UpdateUserTestSuccess(ChangePasswordRequest request, ChangePasswordResponse expectedResponse)
        {
            var user = new ClaimsPrincipal(new ClaimsIdentity(new Claim[]
            {
                new Claim(ClaimTypes.Name, "anuviswan"),

            }, "mock"));


            var userProfileEntity = Mapper.Map<UserProfileEntity>(request);
            var mockUserProfileService = new Mock<IUserProfileService>();
            mockUserProfileService.Setup(x => x.UpdateUserAsync(It.IsAny<UserProfileEntity>()))
                .Returns(Task.FromResult(true));

            var userController = new UserController(Mapper, mockUserProfileService.Object, null, null);
            userController.ControllerContext.HttpContext = new DefaultHttpContext() { User = user };
            MockModelState(request, userController);
            var result = await userController.ChangePassword(request);
            Assert.True(result is IErrorInfo instance && !instance.HasError);
            Assert.True(string.IsNullOrEmpty(result.ErrorMessage));
        }

        public static IEnumerable<object[]> ChangePasswordTestSuccessTestData => new[]
        {
            new object[]{new ChangePasswordRequest { OldPassword="OldPassworld",NewPassword="NewPassword"},new ChangePasswordResponse { } },
        };
    }
}