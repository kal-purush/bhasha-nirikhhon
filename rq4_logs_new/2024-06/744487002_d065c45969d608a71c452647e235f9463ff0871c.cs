using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.SignalR;
using Server.Model;
using Server.Model.Requests.User;

namespace Server.Hub;

public class FriendRequestHub(IDictionary<string, UserRoomConnection> connection, UserManager<ApplicationUser> userManager)
    : Microsoft.AspNetCore.SignalR.Hub
{
    public async Task OnFriendRequestSend(FriendRequest request)
    {
        Console.WriteLine("-----------------------------");
        foreach (var userRoomConnection in connection.Values)
        {
            Console.WriteLine(userRoomConnection.User);
        }
        Console.WriteLine("-----------------------------");
        var receiverConnection = connection.Values.FirstOrDefault(conn => conn.User == request.Receiver);
        if (receiverConnection != null)
        {
            await Clients.Client(receiverConnection.User!).SendAsync("ReceiveFriendRequest", request.SenderId, request.Receiver);
        }
        else
        {
            Console.WriteLine("Receiver connection not found.");
        }
        Console.WriteLine($"Friend request sent from {request.SenderId} to {request.Receiver}");
    }
}