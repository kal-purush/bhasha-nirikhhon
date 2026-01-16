using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;

namespace AM.UMS.BackEnd.Data
{
    public class Item
    {
        [Key]
        public int ItemID { get; set; }

        [Required]
        public int ItemCode { get; set; }

        [Required]
        public string ItemName { get; set; }

        [Required]
        public decimal UnitPrice { get; set; }

        //Navigation Property
        public List<ItemOrder> ItemOrders { get; set; }
    }
}