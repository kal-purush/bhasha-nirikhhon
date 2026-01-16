using Core.Enums;
using MessagePack;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Core.Models
{
    public class CartItem
    {
        public int Id { get; set; }
        public double Price { get; set; }
        public string? Image { get; set; }
        public string? PurchasedFoods { get; set; }
        public double Total { get; set; }
        public int Quantity { get; set; }
        public int? FoodId { get; set; }
        [ForeignKey("FoodId")]
        public virtual Food? Foods { get; set; }
    }
}