using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Client
{
    public class Auctions
    {
        private List<Auction> list;

        public Auctions()
        {
            this.list = new List<Auction>();

        }

        public void add(Auction item)
        {
            this.list.Add(item);
        }

        public void UpdateingAuctionTik(AuctionTik obj)
        {
            for (int i = 0; i < this.list.Count; i++)
                if (this.list[i].ID == obj.ID)
                    lock (this.list[i])
                        this.list[i].AuctionTik = obj.Value;
        }
    }

    public class Auction
    {
        private int itemID;
        private string description;
        private double price;
        
        private int auctionTik;

        public Auction(int id, string description, double price)
        {
            this.itemID = id;
            this.description = description;
            this.price = price;

            this.auctionTik = -1;
        }

        public int ID { get { return this.itemID; } set { this.itemID = value; } }
        public string Description { get { return this.description; } set { this.description = value; } }
        public double Price { get { return this.price; } set { this.price = value; } } 
        public int AuctionTik{ get { return this.auctionTik;} set { this.auctionTik = value; } }
    }

    public class AuctionTik
    {
        private int id;
        private int value;

        public AuctionTik(int id, int value)
        {
            this.id = id;
            this.value = value;
        }

        public int ID { get { return this.id; } }
        public int Value { get { return this.value; } }
    }

    public class AuctionSlot
    {
        private int itemId;
        private int clientId;

        public AuctionSlot(int itemId, int clientId)
        {
            this.itemId = itemId;
            this.clientId = clientId;
        }

        public int ItemId { get { return this.itemId; } }
        public int ClientId { get { return this.clientId; } }
    }
}