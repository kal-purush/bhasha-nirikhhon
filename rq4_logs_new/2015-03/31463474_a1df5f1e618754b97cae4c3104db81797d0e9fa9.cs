using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Threading;

using Auction_Items;

namespace Server
{
    public class Items : Auction_Items.Items
    {
        private const int UPDATE_INTERVAL = 2000;

        private bool runing;

        private ClientList clients;
        private Thread broadcastedTasks;

        public Items() : base()
        {
            this.runing = false;
            this.broadcastedTasks = new Thread(new ThreadStart(BroadcastedtasksTikTask));
            this.broadcastedTasks.Name = "Broadcast tik";
        }

        public void Bind(ref ClientList clients)
        {
            this.clients = clients;
        }

        public void Start()
        {
            if (this.clients == null)
                return;

            if (!this.runing)
                this.runing = true;

            this.broadcastedTasks.Start();
        }

        public void Close()
        {
            this.runing = false;
            this.broadcastedTasks.Join();
        }

        private void BroadcastedtasksTikTask()
        {
            while (runing)
            {
                for (int i = 0; i < this._items.Count; i++)
                {
                    if (!this._items[i].Slot && this._items[i].LastBid != null)
                    {
                        var time = (int)(DateTime.Now - (DateTime)this._items[i].LastBid).TotalSeconds;

                        if (this._items[i].BroadcastedTimes == 0 && time >= 10)
                        {
                            this.clients.Broadcaster("/auctionTik id=" + this._items[i].ID + " value=0");

                            lock (this._items[i])
                                this._items[i].Broadcasted();
                        }
                        else if (this._items[i].BroadcastedTimes == 1 && time >= 20)
                        {
                            this.clients.Broadcaster("/auctionTik id=" + this._items[i].ID + " value=1");
                            
                            lock (this._items[i])
                                this._items[i].Broadcasted();
                        }
                        else if (this._items[i].BroadcastedTimes == 2 && time >= 30)
                        {
                            this.clients.Broadcaster("/auctionSlot itemId=" + this._items[i].ID 
                                + " clientId=" + this._items[i].ToCilnetId);

                            lock (this._items[i])
                                this._items[i].ItemSlot();
                        }
                    }
                }
                
                if (runing)
                    Thread.Sleep(UPDATE_INTERVAL);
            }
        }
    }
}