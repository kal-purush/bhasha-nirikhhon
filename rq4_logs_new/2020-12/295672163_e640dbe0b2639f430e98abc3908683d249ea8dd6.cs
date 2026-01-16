using PSIShoppingEngine.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Type = PSIShoppingEngine.Models.Type;

namespace PSIShoppingEngine.DTOs.ItemPrice
{
    public struct ItemPrices
    {
        public DateTime Date { get; set; }
        public double Price { get; set; }
        public Shop shop { get; set; }
    }
    public class GetPriceOfItemDto
    {
        public Item Item { get; set; }
        public List<ItemPrices> Prices { get; set; }
    }
}