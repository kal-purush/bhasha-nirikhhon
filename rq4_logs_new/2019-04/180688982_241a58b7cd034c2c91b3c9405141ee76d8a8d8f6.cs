using System;
using System.Collections.Generic;
using System.Text;
using Xunit;
using MVCswitchback.Models;

namespace WebAppTests
{
    public class UserReviewModelTests
    {
        [Fact]
        public void CanGetUserReviewID()
        {
            UserReviews userReviews = new UserReviews()
            {
                ID = 1
            };

            Assert.Equal(1, userReviews.ID);
        }

        [Fact]
        public void CanSetUserReviewID()
        {
            UserReviews userReviews = new UserReviews()
            {
                ID = 1
            };
            userReviews.ID = 2;

            Assert.Equal(2, userReviews.ID);
        }

        [Fact]
        public void CanGetUserReviewUserID()
        {
            UserReviews userReviews = new UserReviews()
            {
                UserID = 1
            };

            Assert.Equal(1, userReviews.UserID);
        }

        [Fact]
        public void CanSetUserReviewUserID()
        {
            UserReviews userReviews = new UserReviews()
            {
                UserID = 1
            };
            userReviews.UserID = 2;

            Assert.Equal(2, userReviews.UserID);
        }

        [Fact]
        public void CanGetUserReviewTrailID()
        {
            UserReviews userReviews = new UserReviews()
            {
                TrailID = 1
            };

            Assert.Equal(1, userReviews.TrailID);
        }

        [Fact]
        public void CanSetUserReviewTrailID()
        {
            UserReviews userReviews = new UserReviews()
            {
                TrailID = 1
            };
            userReviews.TrailID = 2;

            Assert.Equal(2, userReviews.TrailID);
        }

        [Fact]
        public void CanGetUserReviewUserComment()
        {
            UserReviews userReviews = new UserReviews()
            {
                UserComment = "Was Fun"
            };

            Assert.Equal("Was Fun", userReviews.UserComment);
        }

        [Fact]
        public void CanSetUserReviewUserComment()
        {
            UserReviews userReviews = new UserReviews()
            {
                UserComment = "Was Fun"
            };
            userReviews.UserComment = "Nevermind";

            Assert.Equal("Nevermind", userReviews.UserComment);
        }
    }
}