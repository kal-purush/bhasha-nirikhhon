using Microsoft.AspNetCore.SignalR;
using System;
using System.Threading.Tasks;

namespace Covid19TemperatureAPI.SignalRHub
{
    public class Covid19Hub : Hub<ICovidHubClient>
    {
        public async Task BroadcastMessage(string message)
        {
            await Clients.All.ReceiveMessage(message);
        }

        public async Task SendToCaller(string message)
        {
            await Clients.Caller.ReceiveMessage(message);
        }

        public async Task SendToOthers(string message)
        {
            await Clients.Others.ReceiveMessage(message);
        }

        public async Task SendToGroup(string groupName, string message)
        {
            await Clients.Group(groupName).ReceiveMessage(message);
        }

        public async Task AddUserToGroup(string groupName)
        {
            await Groups.AddToGroupAsync(Context.ConnectionId, groupName);
            await Clients.Caller.ReceiveMessage($"Current user added to {groupName} group");
            await Clients.Others.ReceiveMessage($"User {Context.ConnectionId} added to {groupName} group");
        }
        public async Task RemoveUserFromGroup(string groupName)
        {
            await Groups.RemoveFromGroupAsync(Context.ConnectionId, groupName);
            await Clients.Caller.ReceiveMessage($"Current user removed from {groupName} group");
            await Clients.Others.ReceiveMessage($"User {Context.ConnectionId} removed from {groupName} group");
        }
        public override async Task OnConnectedAsync()
        {
            await Groups.AddToGroupAsync(Context.ConnectionId, "HubUsers");
            await base.OnConnectedAsync();
        }
        public override async Task OnDisconnectedAsync(Exception exception)
        {
            await Groups.RemoveFromGroupAsync(Context.ConnectionId, "HubUsers");
            await base.OnDisconnectedAsync(exception);
        }
    }
}