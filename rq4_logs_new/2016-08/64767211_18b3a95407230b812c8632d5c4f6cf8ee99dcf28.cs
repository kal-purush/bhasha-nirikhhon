using WebApiFrame.Controller;
using System;
using Xunit;
using Microsoft.AspNetCore.Mvc;
namespace WebApiFrame.UnitTests.Controller
{
    public class UsersController_Get
    {
        private readonly UsersController _userController;
        public UsersController_Get()
         {
             _userController = new UsersController();
         }

        [Fact]
        public void ReturnFalseGivenValueOf1()
        {
            IActionResult result = _userController.Get(2);
            
            Assert.False(result.ToString()=="", $"1 should not be prime");
        }
    }
}