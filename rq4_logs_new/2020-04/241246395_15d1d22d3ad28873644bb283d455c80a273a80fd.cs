using System;
using PulsePI.DataAccess;
using PulsePI.Models;
using Moq;
using Xunit;
using Microsoft.Extensions.Configuration;

namespace PulseTest.DataAccessTests
{
    public class AccountDaoTests : DaoTestBase
    {
        [Fact]
        public void CreateAccount_ShouldNot_Create_IfAccountExists()
        {
            using(var context = new PulsePiDBContext(GetTestOptions()))
            {
                context.Database.EnsureDeleted();

                var createAccountDao = new AccountDao(context);

                var acc = new Account()
                {
                    username = "username",
                    password = "password"
                };

                context.Add(acc);
                context.SaveChanges();

                var response = createAccountDao.CreateAccount(acc);

                Assert.True(response.Exception.InnerException.Message.Equals("Account already exists"));
                Assert.True(response.IsFaulted);

            }
        }
    }
}