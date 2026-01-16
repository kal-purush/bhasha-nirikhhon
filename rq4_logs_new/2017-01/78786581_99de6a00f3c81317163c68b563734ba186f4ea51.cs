using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using System.Web;
using Microsoft.AspNet.SignalR;

namespace ChatroServer
{
    public class ChatHub : Hub
    {
        public override Task OnConnected()
        {
            Debug.WriteLine($"Client connected - connection id: {this.Context.ConnectionId}", "info");
            return base.OnConnected();
        }

        public override Task OnDisconnected(bool stopCalled)
        {
            Debug.WriteLine($"Client disconnected - connection id: {this.Context.ConnectionId}", "info");
            return base.OnDisconnected(stopCalled);
        }

        public void NewBroadcast(string message)
        {
            this.Clients.All.sendBroadcast(message);
        }

        public void NewMessage(string message, string recipient)
        {

        }

        public void JoinRoom(string roomName)
        {

        }
    }
}