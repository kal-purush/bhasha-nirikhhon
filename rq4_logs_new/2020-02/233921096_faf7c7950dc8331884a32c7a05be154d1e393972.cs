using Losol.Identity.Tests.Extensions;
using Moq;
using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Xunit;

namespace Losol.Identity.Tests.Controllers.Account
{
    public class AccountControllerTests : IClassFixture<CustomWebApplicationFactory<Startup>>, IDisposable
    {
        private readonly CustomWebApplicationFactory<Startup> _factory;

        public AccountControllerTests(CustomWebApplicationFactory<Startup> factory)
        {
            _factory = factory;
        }

        public void Dispose()
        {
            _factory.CleanupAsync().Wait();
        }

        [Theory]
        [MemberData(nameof(InvalidPhoneNumbers))]
        public async Task Should_Require_Valid_Phone_Number_To_Send_Verification_Code(string phoneNumber)
        {
            await _factory.CreateClient().SendPhoneVerificationTokenAsync(phoneNumber);

            _factory.SmsSenderMock.Verify(s => s
                .SendSmsAsync(It.IsAny<string>(), It.IsAny<string>()),
                Times.Never);
        }

        [Fact]
        public async Task Should_Send_Sms_With_Phone_Verification_Code_To_Non_Existing_User()
        {
            await _factory.CreateClient().SendPhoneVerificationTokenAsync(ValidPhoneNumber);

            _factory.SmsSenderMock.Verify(s => s
                .SendSmsAsync(ValidPhoneNumber, It.IsAny<string>()),
                Times.Once);
        }

        [Theory]
        [MemberData(nameof(InvalidPhoneNumbers))]
        public async Task Should_Not_Resend_Phone_Verification_Code_To_Invalid_Phone_Number(string invalidPhoneNumber)
        {
            var client = _factory.CreateClient();
            var sendCodeResponse = await client.SendPhoneVerificationTokenAsync(ValidPhoneNumber);

            _factory.SmsSenderMock.Verify(s => s
                .SendSmsAsync(ValidPhoneNumber, It.IsAny<string>()),
                Times.Once);

            _factory.SmsSenderMock.Reset();

            var actionUrl = await sendCodeResponse.GetFormActionUrlAsync();
            var antiForgeryToken = await sendCodeResponse.GetAntiForgeryTokenAsync();
            var returnUrl = await sendCodeResponse.GetHiddenInputValueAsync("ReturnUrl");
            var tokenKey = await sendCodeResponse.GetHiddenInputValueAsync("TokenKey");

            var resendCodeResponse = await client.PostAsync(actionUrl, new Dictionary<string, string>
            {
                { "button", "resend" },
                { "PhoneNumber", invalidPhoneNumber },
                { "ReturnUrl", returnUrl },
                { "TokenKey", tokenKey },
                { HttpResponseMessageExtensions.RequestVerificationTokenParam, antiForgeryToken }
            });
            await resendCodeResponse.CheckIsSuccessfulAsync();

            _factory.SmsSenderMock.Verify(s => s
                .SendSmsAsync(It.IsAny<string>(), It.IsAny<string>()),
                Times.Never);
        }

        [Fact]
        public async Task Should_Allow_To_Resend_Phone_Verification_Code_To_Non_Existing_User()
        {
            var client = _factory.CreateClient();
            var sendCodeResponse = await client.SendPhoneVerificationTokenAsync(ValidPhoneNumber);

            _factory.SmsSenderMock.Verify(s => s
                    .SendSmsAsync(ValidPhoneNumber, It.IsAny<string>()),
                Times.Once);

            _factory.SmsSenderMock.Reset();

            var actionUrl = await sendCodeResponse.GetFormActionUrlAsync();
            var antiForgeryToken = await sendCodeResponse.GetAntiForgeryTokenAsync();
            var returnUrl = await sendCodeResponse.GetHiddenInputValueAsync("ReturnUrl");
            var tokenKey = await sendCodeResponse.GetHiddenInputValueAsync("TokenKey");

            var resendCodeResponse = await client.PostAsync(actionUrl, new Dictionary<string, string>
            {
                { "button", "resend" },
                { "PhoneNumber", ValidPhoneNumber },
                { "ReturnUrl", returnUrl },
                { "TokenKey", tokenKey },
                { HttpResponseMessageExtensions.RequestVerificationTokenParam, antiForgeryToken }
            });
            await resendCodeResponse.CheckIsSuccessfulAsync();

            _factory.SmsSenderMock.Verify(s => s
                    .SendSmsAsync(ValidPhoneNumber, It.IsAny<string>()),
                Times.Once);
        }

        [Fact]
        public async Task Should_Generate_Valid_Verification_Code()
        {
            var client = _factory.CreateClient();

            var message = "";
            _factory.SmsSenderMock.Setup(s => s
                    .SendSmsAsync(ValidPhoneNumber, It.IsAny<string>()))
                .Callback<string, string>((phoneNumber, text) =>
                {
                    message = text;
                });

            // send verification code

            var sendCodeResponse = await client.SendPhoneVerificationTokenAsync(ValidPhoneNumber);
            Assert.NotEmpty(message);

            _factory.SmsSenderMock.Verify(s => s
                .SendSmsAsync(ValidPhoneNumber, message), Times.Once);

            var m = VerificationTokenPattern.Match(message);
            Assert.True(m.Success);

            var verificationCode = m.Groups[1];
            Assert.NotNull(verificationCode);

            // verify phone number

            var actionUrl = await sendCodeResponse.GetFormActionUrlAsync();
            var antiForgeryToken = await sendCodeResponse.GetAntiForgeryTokenAsync();
            var returnUrl = await sendCodeResponse.GetHiddenInputValueAsync("ReturnUrl");
            var tokenKey = await sendCodeResponse.GetHiddenInputValueAsync("TokenKey");

            var verifyCodeResponse = await client.PostAsync(actionUrl, new Dictionary<string, string>
            {
                { "button", "verify" },
                { "PhoneNumber", ValidPhoneNumber },
                { "ReturnUrl", returnUrl },
                { "TokenKey", tokenKey },
                { "SmsCode", verificationCode.ToString() },
                { HttpResponseMessageExtensions.RequestVerificationTokenParam, antiForgeryToken }
            });
            await verifyCodeResponse.CheckIsSuccessfulAsync();
            var html = await verifyCodeResponse.Content.ReadAsStringAsync();
            Assert.Contains("You are now being returned to the application.", html); // FIXME: check for auth code, exchange it with token etc
        }

        private const string ValidPhoneNumber = "+11111111111";

        public static IEnumerable<object[]> InvalidPhoneNumbers => new List<object[]>{
            new object[]{null},
            new object[]{""},
            new object[]{"   "},
            new object[]{"asd"}
        };

        private static readonly Regex VerificationTokenPattern = new Regex("(\\d+)");
    }
}