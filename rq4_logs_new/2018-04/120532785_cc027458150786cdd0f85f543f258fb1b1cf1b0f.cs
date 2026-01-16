using AirlinesManagerGame.Models;
using System;

namespace AirlinesManagerGame.Services.Mediators
{
    public class ItemPurchaseMediator
    {
        public delegate void ItemPurchasedEventHandler(object sender, ItemPurchasedEventArgs e);
        public static event ItemPurchasedEventHandler OnItemPurchased;

        public delegate void AirplanePurchasedEventHandler(object sender, AirplanePurchasedEventArgs e);
        public static event AirplanePurchasedEventHandler OnAirplanePurchased;

        public delegate void AirportPurchasedEventHandler(object sender, AirportPurchasedEventArgs e);
        public static event AirportPurchasedEventHandler OnAirportPurchased;

        public static void AddItem(object sender, StoreItem item)
        {
            OnItemPurchased?.Invoke(sender, new ItemPurchasedEventArgs(item));
        }

        public static void AddAirplane(object sender, Airplane airplane)
        {
            OnAirplanePurchased?.Invoke(sender, new AirplanePurchasedEventArgs(airplane));
            AddItem(sender, airplane);
        }

        public static void AddAirport(object sender, Airport airport)
        {
            OnAirportPurchased?.Invoke(sender, new AirportPurchasedEventArgs(airport));
            AddItem(sender, airport);
        }

        public class ItemPurchasedEventArgs : EventArgs
        {
            public StoreItem PurchasedItem { get; private set; }

            public ItemPurchasedEventArgs(StoreItem _purchasedItem)
            {
                PurchasedItem = _purchasedItem;
            }
        }

        public class AirplanePurchasedEventArgs : ItemPurchasedEventArgs
        {
            public Airplane PurchasedAirplane { get; private set; }

            public AirplanePurchasedEventArgs(Airplane _purchasedAirplane) : base(_purchasedAirplane)
            {
                PurchasedAirplane = _purchasedAirplane;
            }
        }

        public class AirportPurchasedEventArgs : ItemPurchasedEventArgs
        {
            public Airport PurchasedAirport { get; private set; }

            public AirportPurchasedEventArgs(Airport _purchasedAirport) : base(_purchasedAirport)
            {
                PurchasedAirport = _purchasedAirport;
            }
        }


    }
}