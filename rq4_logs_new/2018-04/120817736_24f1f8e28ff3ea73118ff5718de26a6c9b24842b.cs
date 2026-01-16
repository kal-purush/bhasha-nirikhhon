using Microsoft.EntityFrameworkCore;
using NewsfeedClient.Controllers;
using NewsfeedClient.DAL;
using NewsfeedClient.Models;
using NewsfeedClient.Tests.Helpers;
using NUnit.Framework;

namespace NewsfeedClient.Tests
{
    class SubscriptionsControllerTests
    {

        NewsfeedContext fakeContext;
        SubscriptionsController controller;

        [SetUp]
        public void MockControllerSetUp()
        {
            var optionsBuilder = new DbContextOptionsBuilder<NewsfeedContext>();
            optionsBuilder.UseInMemoryDatabase("fakeDb");
            fakeContext = new NewsfeedContext(optionsBuilder.Options);

            fakeContext.Users = DbContextHelper.GetQueryableMockDbSet
                (
                    new User() { Id = 1,
                                 Subscriptions = { new Subscription { UserId = 1, DigestId = 1}
                               }
                    },
                    new User() { Id = 2 }
                );

            fakeContext.Digests = DbContextHelper.GetQueryableMockDbSet
                (
                    new Digest() { Id = 1, CreatorId = 1 },
                    new Digest() { Id = 2, CreatorId = 1 },
                    new Digest() { Id = 3, CreatorId = 2, IsPublic= false }
                );

            controller = new SubscriptionsController(fakeContext);
        }

        [Test]
        public void SubscribeUserToDigest_NonExistingUser_ReturnsNotFound()
        {

        }

        [Test]
        public void SubscribeUserToDigest_NonExistingDigest_ReturnsNotFound()
        {

        }

        [Test]
        public void SubscribeUserToDigest_DigestIsPrivate_Returns401()
        {

        }

        [Test]
        public void SubscribeUserToDigest_AlreadySubscribed_Returns409()
        {

        }

        [Test]
        public void SubscribeUserToDigest_NoErrors_ReturnsOK()
        {

        }

    }
}