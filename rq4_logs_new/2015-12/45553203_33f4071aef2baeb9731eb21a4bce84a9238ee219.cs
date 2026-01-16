using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Microsoft.AspNet.SignalR;
using MinesweepR.Api.Models;
using System.Threading.Tasks;

namespace MinesweepR.Api.Hubs
{
    public class GameHub : Hub
    {
        public static List<Game> GameData { get; set; }

        public GameHub()
        {
            if (GameData == null)
            {
                GameData = new List<Game>();
            }
        }

        public void Hello()
        {
            Clients.All.hello();
        }

        public void PingMotherFlipper (string groupName, string message)
        {
            foreach (var objHub in GameData.Where(a => a.GroupName.Trim().ToLower() == groupName.Trim().ToLower() 
                && a.ConnectionId != Context.ConnectionId))
            {
                    Clients.Client(objHub.ConnectionId).pingingMotherFlipper(message);
            }
        }

        public override Task OnConnected()
        {
            CheckConnection();

            return base.OnConnected();
        }

        public override Task OnDisconnected(bool stopCalled)
        {
            GameData.Remove(GameData.FirstOrDefault(a => a.ConnectionId == Context.ConnectionId));

            return base.OnDisconnected(stopCalled);
        }

        public override Task OnReconnected()
        {
            CheckConnection();
            return base.OnReconnected();
        }

        private void CheckConnection()
        {
            if (!GameData.Any(a => a.ConnectionId == Context.ConnectionId))
            {
                GameData.Add(new Game { ConnectionId = Context.ConnectionId, GroupName = Context.QueryString["GroupName"] });
            }
        }
    }
}