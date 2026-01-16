using System;
using System.Diagnostics;
using System.Linq;
using Xunit;

namespace RefactoringExample
{
    public class AuthenticatorTests {
        private Authenticator authenticator;
        private TraditionalUser traditionalUser;
        private ActiveDirectoryUser adUser;

        public AuthenticatorTests() {
            authenticator = new Authenticator();
            traditionalUser = new TraditionalUser() { Password = "123!", Username = "testuser" };
            adUser = new ActiveDirectoryUser() { Password = "abc!", Username = "testuser" };
        }

        [Theory]
        [InlineData("123!", true)]
        [InlineData("234!", false)]
        public void TraditionalUser_Login_PassesAndFailsBasedOnPassword(string password, bool returnValue) {
            var result = authenticator.Login(traditionalUser, password);
            Assert.Equal(returnValue, result);
        }

        [Theory]
        [InlineData("abc!", true)]
        [InlineData("cde!", false)]
        public void ActiveDirectoryUser_Login_PassesAndFailsBasedOnPassword(string password, bool returnValue) {
            var result = authenticator.Login(adUser, password);
            Assert.Equal(returnValue, result);
        }
    }
}