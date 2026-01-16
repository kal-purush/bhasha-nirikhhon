using IdentityService.FunctionalTests.Setup;
using IdentityServiceAPI.Models;
using Microsoft.AspNetCore.Identity;
using Newtonsoft.Json;
using System.Net;

namespace IdentityService.FunctionalTests
{
    [TestClass]
    public class UserFunctionalities_NgTests : TestScenarioBase
    {
        const string ValidLogin = "TestLogin";
        const string ValidEmail = "test@test.pl";
        const string ValidPassword = "T3sting1!";

        [DataTestMethod] // Test cases to be refactorized with final identity specification
        [DataRow(AdminLogin, ValidEmail, ValidPassword, "DuplicateUserName")]
        [DataRow(ValidLogin, ValidEmail, "Aa1@", "PasswordTooShort")]
        [DataRow(ValidLogin, ValidEmail, "noNoAlphanumr1c", "PasswordRequiresNonAlphanumeric")]
        [DataRow(ValidLogin, ValidEmail, "nouppercase1!", "PasswordRequiresUpper")]
        [DataRow(ValidLogin, ValidEmail, "NOLOWERCASE1!", "PasswordRequiresLower")]
        [DataRow(ValidLogin, ValidEmail, "111@@@@@@1!", "PasswordRequiresLower", "PasswordRequiresUpper")]
        public void RegisterNewUser_ReturnsBadRequestAndIdentityErrors(string login, string email, string password, params string[] expectedErrors)
        {
            var client = GetClient();
            RegisterModel registerModel = new()
            {
                Login = login,
                Email = email,
                Password = password
            };

            HttpRequestMessage request = new()
            {
                RequestUri = new Uri(IdentityServiceUri + $"RegisterNewUser"),
                Method = HttpMethod.Post,
                Headers =
                {
                    {HttpRequestHeader.ContentType.ToString(), "application/json"},
                },
                Content = JsonContent.Create(registerModel)
            };

            var response = client.SendAsync(request).Result;            
            var responseContent = response.Content.ReadAsStringAsync().Result;
            var errors = JsonConvert.DeserializeObject<List<IdentityError>>(responseContent);

            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.IsNotNull(errors);            

            foreach (var expectedError in expectedErrors)
                Assert.IsTrue(errors.Any(e => e.Code.Equals(expectedError)), $"{expectedError} not found");

            Assert.AreEqual(expectedErrors.Length, errors.Count);
        }

        //[TestMethod]
        //public void Login_ReturnsOk() // To be refactorized 
        //{
        //    string login = AdminLogin,
        //        password = AdminPassword;

        //    using var client = GetClient();
        //    HttpRequestMessage request = new()
        //    {
        //        RequestUri = new Uri(IdentityServiceUri + $"Login?login={login}&password={password}"),
        //        Method = HttpMethod.Get
        //    };

        //    var response = client.SendAsync(request).Result;

        //    Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
        //}

        //[TestMethod]
        //public void GetUserByUserName_ReturnsUser()
        //{
        //    string userName = AdminLogin;

        //    using var client = GetClient();
        //    HttpRequestMessage request = new()
        //    {
        //        RequestUri = new Uri(IdentityServiceUri + $"GetUserByUserName?userName={userName}"),
        //        Method = HttpMethod.Get
        //    };

        //    var response = client.SendAsync(request).Result;

        //    var responseContent = response.Content.ReadAsStringAsync().Result;
        //    var user = JsonConvert.DeserializeObject<ApplicationUserExternalModel>(responseContent);

        //    Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
        //    Assert.IsNotNull(user);
        //    Assert.AreEqual(AdminLogin, user.UserName);
        //}

        //[TestMethod]
        //public void GetRoleByName_ReturnsOkAndRoleName()
        //{
        //    string roleName = AdminRoleName;
        //    using var client = GetClient();
        //    HttpRequestMessage request = new()
        //    {
        //        RequestUri = new Uri(IdentityServiceUri + $"GetRoleByName?roleName={roleName}"),
        //        Method = HttpMethod.Get
        //    };

        //    var response = client.SendAsync(request).Result;
        //    var responseContent = response.Content.ReadAsStringAsync().Result;
        //    var role = JsonConvert.DeserializeObject<IdentityRole>(responseContent);

        //    Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
        //    Assert.IsNotNull(role);
        //    Assert.AreEqual(roleName, role.Name);
        //}

        //[TestMethod]
        //public void GetAllRoles_ReturnsRolesList()
        //{
        //    using var client = GetClient();
        //    HttpRequestMessage request = new()
        //    {
        //        RequestUri = new Uri(IdentityServiceUri + $"GetAllRoles"),
        //        Method = HttpMethod.Get
        //    };

        //    var response = client.SendAsync(request).Result;

        //    var responseContent = response.Content.ReadAsStringAsync().Result;
        //    var roles = JsonConvert.DeserializeObject<List<IdentityRole>>(responseContent);

        //    Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
        //    Assert.IsNotNull(roles);
        //    Assert.AreEqual(1, roles.Count);
        //    Assert.AreEqual(AdminRoleName, roles[0].Name);
        //}

        //[TestMethod]
        //public void AddUserToRole_ShouldReturnOk()
        //{
        //    string userName = NoRoleUserLogin,
        //        roleName = AdminRoleName;

        //    var client = GetClient();
        //    HttpRequestMessage request = new()
        //    {
        //        RequestUri = new Uri(IdentityServiceUri + $"AddUserToRole?userName={userName}&roleName={roleName}"),
        //        Method = HttpMethod.Patch
        //    };

        //    var response = client.SendAsync(request).Result;

        //    Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
        //}

        //[TestMethod]
        //public void RemoveUserFromRole_ShouldReturnOk()
        //{
        //    string userName = AdminRoleName,
        //        roleName = AdminRoleName;

        //    var client = GetClient();
        //    HttpRequestMessage request = new()
        //    {
        //        RequestUri = new Uri(IdentityServiceUri + $"RemoveUserFromRole?userName={userName}&roleName={roleName}"),
        //        Method = HttpMethod.Patch
        //    };

        //    var response = client.SendAsync(request).Result;

        //    Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
        //}
    }
}