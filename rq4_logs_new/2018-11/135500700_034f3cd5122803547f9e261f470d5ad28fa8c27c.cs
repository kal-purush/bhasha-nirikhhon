using System;
using System.Threading.Tasks;
using BankWithUs.Controllers;
using BankWithUs.Models;
using Microsoft.AspNetCore.Mvc;
using Xunit;

namespace BankWithUs.Tests
{
    public class AccountTests
    {
        [Fact]
        public async Task Test_Account_Create()
        {
            AccountsController controller = new AccountsController();

            IActionResult result = await controller.Create(new Account() { Name = "Test", SSN = "078-05-1120" });
            Assert.True(result is ViewResult);
            var response = (result as ViewResult);
            var responseItem = response.Model as Account;
            Assert.True(responseItem.Name == "Test");
            Assert.True(responseItem.SSN == "078-05-1120");
            Assert.True(responseItem.DateCreated != null);
        }
    }
}