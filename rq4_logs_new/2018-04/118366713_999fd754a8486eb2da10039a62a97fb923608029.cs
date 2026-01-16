using System;
using System.Collections.Generic;

namespace LegendaryStore.Entities
{
    public class Order
    {
        public int OrderId { get; set; }
        public string Username { get; set; }
        public DateTime CheckoutBegan { get; set; }
        public DateTime? OrderPlaced { get; set; }
        public decimal Total { get; set; }
        public OrderState Status { get; set; }

        public string ShippingZipCode { get; set; }
        public string ShippingCountry { get; set; }
        public string ShippingState { get; set; }
        public string ShippingCity { get; set; }
        public string ShippingAddress { get; set; }

        public List<OrderLine> Lines { get; set; }
    }

    public enum OrderState
    {
        CheckingOut,
        Placed,
        Filling,
        ReadyToShip,
        Shipped,
        Delivered,
        Cancelled
    }

    public static class OrderStateExtensions
    {
        public static string DisplayName(this OrderState state)
        {
            switch (state)
            {
                case OrderState.Placed: return "Ready to Pack";
                case OrderState.Filling: return "Being Packed";
                case OrderState.ReadyToShip: return "Ready to Ship";
                default: return state.ToString();
            }
        }
    }
}