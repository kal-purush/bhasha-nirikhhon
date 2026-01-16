using System;
using System.Collections.Generic;
using System.Linq;

namespace SimpleJoins
{
    public class DataWarehouse
    {
        public List<Item> Items { get; set; }
        public List<Purchase> Purchases { get; set; }

        public DataWarehouse()
        {
            Items = new List<Item>
            {
                new Item { ItemId = 1, ItemName = "Biscuit" },
                new Item { ItemId = 2, ItemName = "Chocolate" },
                new Item { ItemId = 3, ItemName = "Butter" },
                new Item { ItemId = 4, ItemName  = "Bread" },
                new Item { ItemId = 5, ItemName = "Honey" }
            };
            Purchases = new List<Purchase>
            {
                new Purchase { InvoiceNo =100, ItemId = 3,  PurchaseQuantity = 800 },
                new Purchase { InvoiceNo =101, ItemId = 2,  PurchaseQuantity = 650 },
                new Purchase { InvoiceNo =102, ItemId = 3,  PurchaseQuantity = 900 },
                new Purchase { InvoiceNo =103, ItemId = 4,  PurchaseQuantity = 700 },
                new Purchase { InvoiceNo =104, ItemId = 3,  PurchaseQuantity = 900 },
                new Purchase { InvoiceNo =105, ItemId = 4,  PurchaseQuantity = 650 },
                new Purchase { InvoiceNo =106, ItemId = 1,  PurchaseQuantity = 458 }
            };
        }

        public List<T> GetData<T>() where T : class
        {
            var data = (List<T>) GetType().GetProperties().FirstOrDefault(p => p.PropertyType == typeof(List<T>))?.GetValue(this, null);

            if (data == null)
            {
                throw new ArgumentException("Invalid data type! No such data in warehouse!", nameof(T));
            }

            return data;
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            
        }
    }
}