using Microsoft.AspNetCore.Identity;
using Microsoft.EntityFrameworkCore;
using Moq;
using Server.Database;
using Server.Model;
using Server.Model.Requests.User;
using Server.Services.FriendConnection;
using Server.Services.User;

namespace Tests.ServicesTests.FriendConnection;

public class FriendConnectionServiceTests
{
    private DbContextOptions<DatabaseContext> options;
    private Mock<IUserServices> _mockUserServices;

    [SetUp]
    public void Setup()
    {
        options = new DbContextOptionsBuilder<DatabaseContext>()
            .UseInMemoryDatabase(databaseName: "Test_Database")
            .Options;
        
        _mockUserServices = new Mock<IUserServices>();
    }

    [Test]
    public async Task GetFriendRequestByIdAsync_ValidRequestId_ReturnsFriendConnection()
    {
        await using var context = new DatabaseContext(options);
        var requestGuid = Guid.NewGuid();
        var friendConnection = new Server.Model.FriendConnection
        {
            ConnectionId = requestGuid,
            SenderId = Guid.NewGuid(),
            ReceiverId = Guid.NewGuid(),
            SentTime = DateTime.UtcNow
        };

        context.FriendConnections?.Add(friendConnection);
        await context.SaveChangesAsync();

        var friendConnectionService = new FriendConnectionService(context, null);

        var result = await friendConnectionService.GetFriendRequestByIdAsync(requestGuid.ToString());

        Assert.That(result, Is.Not.Null);
        Assert.That(result.ConnectionId, Is.EqualTo(requestGuid));
    }

    [Test]
    public async Task GetPendingReceivedFriendRequests_ReturnsPendingRequests()
    {
        await using var context = new DatabaseContext(options);
        var user1Id = Guid.NewGuid().ToString();
        var user2Id = Guid.NewGuid().ToString();
        var user3Id = Guid.NewGuid().ToString();
        var user1 = new ApplicationUser
        {
            Id = new Guid(user1Id),
            UserName = "TestUsername1"
        };
        var user2 = new ApplicationUser
        {
            Id = new Guid(user2Id),
            UserName = "TestUsername2"

        };
        var user3 = new ApplicationUser
        {
            Id = new Guid(user3Id),
            UserName = "TestUsername3"

        };
        context.Users.Add(user1);
        context.Users.Add(user2);
        context.Users.Add(user3);
        await context.SaveChangesAsync();

        var friendConnectionService = new FriendConnectionService(context, null);

        var friendRequest1 = new FriendRequest(user1Id, user2Id);
        await friendConnectionService.SendFriendRequest(friendRequest1);
        await context.SaveChangesAsync();

        var result = await friendConnectionService.GetPendingReceivedFriendRequests(user2Id);

        Assert.That(result, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(result.Count(), Is.EqualTo(1));
        });
        
        var friendRequest2 = new FriendRequest(user3Id, user2Id);
        await friendConnectionService.SendFriendRequest(friendRequest2);
        await context.SaveChangesAsync();
        
        var result2 = await friendConnectionService.GetPendingReceivedFriendRequests(user2Id);
        Assert.Multiple(() =>
        {
            Assert.That(result2.Count(), Is.EqualTo(2));
        });
    }

    [Test]
    public async Task GetPendingRequestCount_ReturnsCount()
    {
        await using var context = new DatabaseContext(options);
        var userId = Guid.NewGuid().ToString();
        var user = new ApplicationUser
        {
            Id = new Guid(userId),
            ReceivedFriendRequests = new List<Server.Model.FriendConnection>
            {
                new(),
                new(),
                new()
            }
        };
        context.Users.Add(user);
        await context.SaveChangesAsync();

        var friendConnectionService = new FriendConnectionService(context, null);

        var result = await friendConnectionService.GetPendingRequestCount(userId);

        Assert.That(result, Is.EqualTo(3));
    }
    
    [Test]
    public async Task GetPendingSentFriendRequests_ReturnsPendingRequests()
    {
        await using var context = new DatabaseContext(options);
        var user1Id = Guid.NewGuid().ToString();
        var user2Id = Guid.NewGuid().ToString();
        var user3Id = Guid.NewGuid().ToString();
        var user1 = new ApplicationUser
        {
            Id = new Guid(user1Id),
            UserName = "TestUsername1"
        };
        var user2 = new ApplicationUser
        {
            Id = new Guid(user2Id),
            UserName = "TestUsername2"

        };
        var user3 = new ApplicationUser
        {
            Id = new Guid(user3Id),
            UserName = "TestUsername3"

        };
        context.Users.Add(user1);
        context.Users.Add(user2);
        context.Users.Add(user3);
        await context.SaveChangesAsync();

        var friendConnectionService = new FriendConnectionService(context, null);

        var friendRequest1 = new FriendRequest(user1Id, user2Id);
        await friendConnectionService.SendFriendRequest(friendRequest1);
        await context.SaveChangesAsync();

        var result = await friendConnectionService.GetPendingSentFriendRequests(user1Id);

        Assert.That(result, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(result.Count(), Is.EqualTo(1));
        });
            
        var friendRequest2 = new FriendRequest(user1Id, user3Id);
        await friendConnectionService.SendFriendRequest(friendRequest2);
        await context.SaveChangesAsync();
            
        var result2 = await friendConnectionService.GetPendingSentFriendRequests(user1Id);
        Assert.Multiple(() =>
        {
            Assert.That(result2.Count(), Is.EqualTo(2));
        });
    }
    
    [Test]
    public async Task AlreadySentFriendRequest_ReturnsPendingRequests()
    {
        await using var context = new DatabaseContext(options);
        var user1Id = Guid.NewGuid().ToString();
        var user2Id = Guid.NewGuid().ToString();
        var user3Id = Guid.NewGuid().ToString();
        var user1 = new ApplicationUser
        {
            Id = new Guid(user1Id),
            UserName = "TestUsername1"
        };
        var user2 = new ApplicationUser
        {
            Id = new Guid(user2Id),
            UserName = "TestUsername2"

        };
        var user3 = new ApplicationUser
        {
            Id = new Guid(user3Id),
            UserName = "TestUsername3"

        };
        context.Users.Add(user1);
        context.Users.Add(user2);
        context.Users.Add(user3);
        await context.SaveChangesAsync();

        var friendConnectionService = new FriendConnectionService(context, null);

        var friendRequest1 = new FriendRequest(user1Id, user2Id);
        await friendConnectionService.SendFriendRequest(friendRequest1);
        await context.SaveChangesAsync();
        
        await friendConnectionService.GetPendingSentFriendRequests(user1Id);
            
        var result = await friendConnectionService.AlreadySentFriendRequest(friendRequest1);
        Assert.Multiple(() =>
        {
            Assert.That(result, Is.EqualTo(true));
        });
    }
    
    [Test]
    public async Task AcceptFriendRequest_ReturnsTrue()
    {
        await using var context = new DatabaseContext(options);
        var user1Id = Guid.NewGuid().ToString();
        var user2Id = Guid.NewGuid().ToString();
        var user3Id = Guid.NewGuid().ToString();
        var user1 = new ApplicationUser
        {
            Id = new Guid(user1Id),
            UserName = "TestUsername1"
        };
        var user2 = new ApplicationUser
        {
            Id = new Guid(user2Id),
            UserName = "TestUsername2"

        };
        var user3 = new ApplicationUser
        {
            Id = new Guid(user3Id),
            UserName = "TestUsername3"

        };
        context.Users.Add(user1);
        context.Users.Add(user2);
        context.Users.Add(user3);
        await context.SaveChangesAsync();

        var friendConnectionService = new FriendConnectionService(context, null);

        var friendRequest1 = new FriendRequest(user1Id, user2Id);
        await friendConnectionService.SendFriendRequest(friendRequest1);
        await context.SaveChangesAsync();

        var result = await friendConnectionService.GetPendingReceivedFriendRequests(user2Id);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.Not.Null);
            Assert.That(result.Count(), Is.EqualTo(1));
        });
        
        var acceptedFriendRequest = await friendConnectionService.AcceptReceivedFriendRequest(result.ToList()[0].RequestId.ToString(), user2Id);
        
        Assert.Multiple(() =>
        {
            Assert.That(result.Count(), Is.EqualTo(1));
            Assert.That(acceptedFriendRequest, Is.True);
        });
    }
    
    [Test]
    public async Task DeleteSentFriendRequest_ReturnsTrue()
    {
        await using var context = new DatabaseContext(options);
        var user1Id = Guid.NewGuid().ToString();
        var user2Id = Guid.NewGuid().ToString();
        var requestGuid = Guid.NewGuid();

        var user1 = new ApplicationUser
        {
            Id = new Guid(user1Id),
            UserName = "TestUsername1",
            SentFriendRequests = new List<Server.Model.FriendConnection>
            {
                new()
                {
                    ConnectionId = requestGuid,
                    SenderId = new Guid(user1Id),
                    ReceiverId = new Guid(user2Id),
                    SentTime = DateTime.UtcNow,
                }
            }
        };
        var user2 = new ApplicationUser
        {
            Id = new Guid(user2Id),
            UserName = "TestUsername2",
        };

        context.Users.AddRange(user1, user2);
        await context.SaveChangesAsync();

        _mockUserServices.Setup(us => us.GetUserWithSentRequests(user1Id))
            .ReturnsAsync(user1);

        var friendConnectionService = new FriendConnectionService(context, _mockUserServices.Object);

        var result = await friendConnectionService.DeleteSentFriendRequest(requestGuid.ToString(), user1Id);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.True);
            Assert.That(user1.SentFriendRequests.First().Status, Is.EqualTo(FriendStatus.Declined));
        });
    }
    
    [Test]
    public async Task DeclineReceivedFriendRequest_ReturnsTrue()
    {
        await using var context = new DatabaseContext(options);
        var user1Id = Guid.NewGuid().ToString();
        var user2Id = Guid.NewGuid().ToString();
        var requestGuid = Guid.NewGuid();

        var user1 = new ApplicationUser
        {
            Id = new Guid(user1Id),
            UserName = "TestUsername1"
        };
        var user2 = new ApplicationUser
        {
            Id = new Guid(user2Id),
            UserName = "TestUsername2",
            ReceivedFriendRequests = new List<Server.Model.FriendConnection>
            {
                new()
                {
                    ConnectionId = requestGuid,
                    SenderId = new Guid(user1Id),
                    ReceiverId = new Guid(user2Id),
                    SentTime = DateTime.UtcNow,
                }
            }
        };

        context.Users.AddRange(user1, user2);
        await context.SaveChangesAsync();

        _mockUserServices.Setup(us => us.GetUserWithReceivedRequests(user2Id))
            .ReturnsAsync(user2);

        var friendConnectionService = new FriendConnectionService(context, _mockUserServices.Object);

        var result = await friendConnectionService.DeclineReceivedFriendRequest(requestGuid.ToString(), user2Id);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.True);
            Assert.That(user2.ReceivedFriendRequests.First().Status, Is.EqualTo(FriendStatus.Declined));
        });
    }
    
    [Test]
    public async Task DeleteFriend_ReturnsTrue()
    {
        await using var context = new DatabaseContext(options);
        var user1Id = Guid.NewGuid().ToString();
        var user2Id = Guid.NewGuid().ToString();
        var user3Id = Guid.NewGuid().ToString();
        var user1 = new ApplicationUser
        {
            Id = new Guid(user1Id),
            UserName = "TestUsername1"
        };
        var user2 = new ApplicationUser
        {
            Id = new Guid(user2Id),
            UserName = "TestUsername2"

        };
        var user3 = new ApplicationUser
        {
            Id = new Guid(user3Id),
            UserName = "TestUsername3"

        };
        context.Users.Add(user1);
        context.Users.Add(user2);
        context.Users.Add(user3);
        await context.SaveChangesAsync();

        var friendConnectionService = new FriendConnectionService(context, null);

        var friendRequest1 = new FriendRequest(user1Id, user2Id);
        await friendConnectionService.SendFriendRequest(friendRequest1);
        await context.SaveChangesAsync();

        var result = await friendConnectionService.GetPendingReceivedFriendRequests(user2Id);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.Not.Null);
            Assert.That(result.Count(), Is.EqualTo(1));
        });
        
        var acceptedFriendRequest = await friendConnectionService.AcceptReceivedFriendRequest(result.ToList()[0].RequestId.ToString(), user2Id);
        var getTestUserFriendsBeforeDelete = await friendConnectionService.GetFriends(user1Id);
        
        Assert.Multiple(() =>
        {
            Assert.That(result.Count(), Is.EqualTo(1));
            Assert.That(acceptedFriendRequest, Is.True);
            Assert.That(getTestUserFriendsBeforeDelete.Count(), Is.EqualTo(1));
        });
        
         await friendConnectionService.DeleteFriend(result.ToList()[0].RequestId.ToString());
        var getTestUserFriendsAfterDelete = await friendConnectionService.GetFriends(user1Id);
        
        Assert.That(getTestUserFriendsAfterDelete.Count(), Is.EqualTo(0));
    }
}