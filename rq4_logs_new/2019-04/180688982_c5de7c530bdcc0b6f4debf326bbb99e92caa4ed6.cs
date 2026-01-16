using System;
using System.Collections.Generic;
using System.Text;
using Xunit;
using MVCswitchback.Models;

namespace WebAppTests
{
   public class UserInfoModelTests
    {
        [Fact]
        public void CanGetUserInfoID()
        {
            UserInfo userInfo = new UserInfo()
            {
                ID = 1
            };

            Assert.Equal(1, userInfo.ID);
        }

        [Fact]
        public void CanSetUserInfoID()
        {
            UserInfo userInfo = new UserInfo()
            {
                ID = 1
            };
            userInfo.ID = 2;

            Assert.Equal(2, userInfo.ID);
        }

        [Fact]
        public void CanGetUserInfoUserName()
        {
            UserInfo userInfo = new UserInfo()
            {
                UserName = "igiffy"
            };

            Assert.Equal("igiffy", userInfo.UserName);
        }

        [Fact]
        public void CanSetUserInfoUserName()
        {
            UserInfo userInfo = new UserInfo()
            {
                UserName = "igiffy"
            };
            userInfo.UserName = "igiffy";

            Assert.Equal("igiffy", userInfo.UserName);
        }

        [Fact]
        public void CanGetUserInfoFirstName()
        {
            UserInfo userInfo = new UserInfo()
            {
                FirstName = "instantiate"
            };

            Assert.Equal("instantiate", userInfo.FirstName);
        }

        [Fact]
        public void CanSetUserInfoFirstName()
        {
            UserInfo userInfo = new UserInfo()
            {
                FirstName = "instantiate"
            };
            userInfo.FirstName = "instantiate";

            Assert.Equal("instantiate", userInfo.FirstName);
        }

        [Fact]
        public void CanGetUserInfoLastName()
        {
            UserInfo userInfo = new UserInfo()
            {
                LastName = "giffy"
            };

            Assert.Equal("giffy", userInfo.LastName);
        }

        [Fact]
        public void CanSetUserInfoLastName()
        {
            UserInfo userInfo = new UserInfo()
            {
                LastName = "giffy"
            };
            userInfo.LastName = "giffy";

            Assert.Equal("giffy", userInfo.LastName);
        }


    }
}