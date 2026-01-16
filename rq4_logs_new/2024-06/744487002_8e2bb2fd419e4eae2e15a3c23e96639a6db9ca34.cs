using System.Collections.Concurrent;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.SignalR;
using MockQueryable.Moq;
using Moq;
using Server.Hub;
using Server.Model;
using Server.Model.Requests.User;
using Server.Services.FriendConnection;
using Xunit;
using Assert = Xunit.Assert;

namespace Tests.HubTests;

public class FriendRequestHubTests
{
    private readonly Mock<IHubCallerClients> _clientsMock;
    private readonly Mock<ISingleClientProxy> _clientProxyMock;
    private readonly Mock<HubCallerContext> _contextMock;
    private readonly Mock<UserManager<ApplicationUser>> _userManagerMock;
    private readonly Mock<IFriendConnectionService> _friendConnectionMock;
    private readonly FriendRequestHub _friendRequestHub;

    public FriendRequestHubTests()
    {
        // Initialize mocks
        _clientsMock = new Mock<IHubCallerClients>();
        _clientProxyMock = new Mock<ISingleClientProxy>();
        _contextMock = new Mock<HubCallerContext>();
        _friendConnectionMock = new Mock<IFriendConnectionService>();

        var store = new Mock<IUserStore<ApplicationUser>>();
        _userManagerMock = new Mock<UserManager<ApplicationUser>>(store.Object, null, null, null, null, null, null, null, null);

        // Initialize the hub with mocked dependencies
        _friendRequestHub = new FriendRequestHub(_userManagerMock.Object, _friendConnectionMock.Object)
        {
            Clients = _clientsMock.Object,
            Context = _contextMock.Object
        };
    }

    [Fact]
    public async Task SendFriendRequest_SendsRequestToReceiverAndSender()
    {
        var requestId = Guid.NewGuid().ToString();
        const string senderName = "TestUser";
        var senderId = Guid.NewGuid();
        var sentTime = DateTime.Now.ToString();
        const string receiverName = "testUser";

        _contextMock.SetupGet(c => c.ConnectionId).Returns("connection123");

        _clientsMock.Setup(c => c.Client(It.IsAny<string>())).Returns(_clientProxyMock.Object);

        var applicationUser1 = new ApplicationUser { UserName = senderName, Id = senderId };
        var applicationUser2 = new ApplicationUser { UserName = receiverName, Id = new Guid("38db530c-b6bb-4e8a-9c19-a5cd4d0fa916") };
        var users = new List<ApplicationUser> { applicationUser1, applicationUser2 }.AsQueryable().BuildMock();

        _userManagerMock.Setup(um => um.Users).Returns(users);
        _userManagerMock.Setup(um => um.FindByNameAsync(It.IsAny<string>()))
            .ReturnsAsync((string name) => users.FirstOrDefault(u => u.UserName == name));

        var testConnections = new ConcurrentDictionary<string, string>();
        testConnections.TryAdd(applicationUser1.Id.ToString(), "senderConnectionId");
        testConnections.TryAdd(applicationUser2.Id.ToString(), "receiverConnectionId");
        FriendRequestHub.Connections = testConnections;

        await _friendRequestHub.SendFriendRequest(requestId, senderName, senderId.ToString(), sentTime, receiverName);

        _clientProxyMock.Verify(
            c => c.SendCoreAsync("ReceiveFriendRequest", 
                It.Is<object[]>(o => 
                    o != null && 
                    o.Length == 6 &&
                    o[0].ToString() == requestId &&
                    o[1].ToString() == senderName &&
                    o[2].ToString() == senderId.ToString() &&
                    o[3].ToString() == sentTime &&
                    o[4].ToString() == receiverName &&
                    o[5].ToString() == applicationUser2.Id.ToString()),
                default), 
            Times.Exactly(2));
    }
    
    [Fact]
    public async Task OnDisconnectedAsync_RemovesConnectionFromDictionary()
    {
        const string connectionId = "connection123";
        _contextMock.SetupGet(c => c.ConnectionId).Returns(connectionId);
        FriendRequestHub.Connections["user123"] = connectionId;

        await _friendRequestHub.OnDisconnectedAsync(null);

        Assert.DoesNotContain("user123", FriendRequestHub.Connections.Keys);
    }

    [Fact]
    public async Task JoinToHub_AddsUserToConnectionsDictionary()
    {
        const string userId = "user123";
        _contextMock.SetupGet(c => c.ConnectionId).Returns("connection123");

        await _friendRequestHub.JoinToHub(userId);

        Assert.Contains(userId, FriendRequestHub.Connections.Keys);
        Assert.Equal("connection123", FriendRequestHub.Connections[userId]);
    }
    
    [Fact]
    public async Task AcceptFriendRequest_SendsAccepted()
    {
        var requestId = Guid.NewGuid().ToString();
        const string senderName = "TestUser";
        var senderId = Guid.NewGuid().ToString();
        var sentTime = DateTime.Now.ToString();
        const string receiverName = "testUser";

        _contextMock.SetupGet(c => c.ConnectionId).Returns("connection123");

        _clientsMock.Setup(c => c.Client(It.IsAny<string>())).Returns(_clientProxyMock.Object);

        var applicationUser1 = new ApplicationUser { UserName = senderName, Id = Guid.Parse(senderId) };
        var applicationUser2 = new ApplicationUser { UserName = receiverName, Id = new Guid("38db530c-b6bb-4e8a-9c19-a5cd4d0fa916") };
        var users = new List<ApplicationUser> { applicationUser1, applicationUser2 }.AsQueryable().BuildMock();

        _userManagerMock.Setup(um => um.Users).Returns(users);
        _userManagerMock.Setup(um => um.FindByNameAsync(It.IsAny<string>()))
            .ReturnsAsync((string name) => users.FirstOrDefault(u => u.UserName == name));

        var testConnections = new ConcurrentDictionary<string, string>();
        testConnections.TryAdd(senderId, "senderConnectionId");
        FriendRequestHub.Connections = testConnections;

        await _friendRequestHub.AcceptFriendRequest(requestId, senderName, senderId, sentTime, receiverName);

        _clientProxyMock.Verify(
            c => c.SendCoreAsync("AcceptFriendRequest", 
                It.Is<object[]>(o => 
                    o != null && 
                    o.Length == 6 &&
                    o[0].ToString() == requestId &&
                    o[1].ToString() == senderName &&
                    o[2].ToString() == senderId &&
                    o[3].ToString() == sentTime &&
                    o[4].ToString() == receiverName &&
                    o[5].ToString() == applicationUser2.Id.ToString()), 
                default), 
            Times.Once);
    }
    
    [Fact]
    public async Task DeclineFriendRequest_SendsDeclineToSenderAndReceiver()
    {
        var requestId = Guid.NewGuid().ToString();
        var senderId = Guid.NewGuid();
        var receiverId = Guid.NewGuid();

        var returningRequest = new FriendConnection(senderId, receiverId);

        _friendConnectionMock.Setup(fcs => fcs.GetFriendRequestByIdAsync(requestId))
            .ReturnsAsync(returningRequest);

        _clientsMock.Setup(c => c.Client(It.IsAny<string>())).Returns(_clientProxyMock.Object);

        var testConnections = new ConcurrentDictionary<string, string>();
        testConnections.TryAdd(senderId.ToString(), "senderConnectionId");
        testConnections.TryAdd(receiverId.ToString(), "receiverConnectionId");
        FriendRequestHub.Connections = testConnections;

        await _friendRequestHub.DeclineFriendRequest(requestId);

        _clientProxyMock.Verify(
            c => c.SendCoreAsync("DeclineFriendRequest",
                It.Is<object[]>(o =>
                    o != null &&
                    o.Length == 1 &&
                    o[0].ToString() == requestId),
                default),
            Times.Exactly(2));
    }
    
    [Fact]
    public async Task DeleteFriend_SendsDeleteToSenderAndReceiver()
    {
        var requestId = Guid.NewGuid().ToString();
        var senderId = Guid.NewGuid().ToString();
        var receiverId = Guid.NewGuid().ToString();

        _clientsMock.Setup(c => c.Client(It.IsAny<string>())).Returns(_clientProxyMock.Object);

        var testConnections = new ConcurrentDictionary<string, string>();
        testConnections.TryAdd(senderId, "senderConnectionId");
        testConnections.TryAdd(receiverId, "receiverConnectionId");
        FriendRequestHub.Connections = testConnections;

        await _friendRequestHub.DeleteFriend(requestId, receiverId, senderId);

        _clientProxyMock.Verify(
            c => c.SendCoreAsync("DeleteFriend",
                It.Is<object[]>(o =>
                    o != null &&
                    o.Length == 1 &&
                    o[0].ToString() == requestId),
                default),
            Times.Exactly(2));
    }
}