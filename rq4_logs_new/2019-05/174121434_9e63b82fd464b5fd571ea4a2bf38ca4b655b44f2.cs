using System;
using System.Collections.Generic;
using System.Reflection.Metadata;
using System.Text;
using System.Threading.Tasks;
using DfE.EmployerFavourites.ApplicationServices.Commands;
using Microsoft.Extensions.Logging;
using Moq;
using SFA.DAS.EAS.Account.Api.Client;
using SFA.DAS.EAS.Account.Api.Types;
using Xunit;

namespace DfE.EmployerFavourites.UnitTests.ApplicationServices.Infrastructure
{
    public class EmployerAccountApiRepositoryTests
    {
        private EmployerAccountApiRepository _sut;
        private readonly Mock<IAccountApiClient> _mockAccountApiClient;
        private readonly Mock<ILogger<EmployerAccountApiRepository>> _mockLogger;
        public const string EMPLOYER_ACCOUNT_ID = "XXX123";
        public const string USER_ID_VALID = "User123";
        public const string USER_ID_INVALID = "User456";

        public EmployerAccountApiRepositoryTests()
        {
            _mockLogger = new Mock<ILogger<EmployerAccountApiRepository>>();
            _mockAccountApiClient = new Mock<IAccountApiClient>();
            _mockAccountApiClient.Setup(x => x.GetUserAccounts(USER_ID_VALID)).ReturnsAsync(GetListOfAccounts());
            _mockAccountApiClient.Setup(x => x.GetUserAccounts(USER_ID_INVALID)).Throws<Exception>();

            _sut = new EmployerAccountApiRepository(_mockAccountApiClient.Object,_mockLogger.Object);
        }

        [Fact]
        public async Task Get_WhenValidUserID_Returns_EmployerAccountId()
        {
            var accountId = await _sut.GetEmployerAccountId(USER_ID_VALID);

            Assert.Equal(EMPLOYER_ACCOUNT_ID,accountId);

        }

        [Fact]
        public async Task Get_WhenInValidUserID_Returns_Exception()
        {
           await Assert.ThrowsAsync<Exception>(() => _sut.GetEmployerAccountId(USER_ID_INVALID));

        }

        [Fact]
        public async Task Get_WhenNullUserID_Returns_ArgumentException()
        {
            await Assert.ThrowsAsync<ArgumentException>(() => _sut.GetEmployerAccountId(null));

        }

        [Fact]
        public async Task Get_WhenWhitespaceUserID_Returns_ArgumentException()
        {
            await Assert.ThrowsAsync<ArgumentException>(() => _sut.GetEmployerAccountId(""));

        }
        private static List<AccountDetailViewModel> GetListOfAccounts()
        {

            return new List<AccountDetailViewModel>
            {
                // Account API in TEST not currently values back correctly. Will rely on the order being deterministic from 
                // the api with the first item being the oldest.
                // new AccountDetailViewModel { HashedAccountId = "ABC123", DateRegistered = new DateTime(2019, 4, 1) },
                // new AccountDetailViewModel { HashedAccountId = "XYZ123", DateRegistered = new DateTime(2019, 3, 1) },
                // new AccountDetailViewModel { HashedAccountId = "XXX123", DateRegistered = new DateTime(2019, 3, 1) },
                // new AccountDetailViewModel { HashedAccountId = "AAA123", DateRegistered = new DateTime(2019, 4, 1) }
                new AccountDetailViewModel { HashedAccountId = "XXX123", DateRegistered = new DateTime(2019, 3, 1) },
                new AccountDetailViewModel { HashedAccountId = "ABC123", DateRegistered = new DateTime(2019, 4, 1) },
                new AccountDetailViewModel { HashedAccountId = "XYZ123", DateRegistered = new DateTime(2019, 3, 1) },
                new AccountDetailViewModel { HashedAccountId = "AAA123", DateRegistered = new DateTime(2019, 4, 1) }
            };
        }



    }
}