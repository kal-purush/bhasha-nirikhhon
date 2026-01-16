using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DTO
{
    public class Food
    {
        private int foodID;
        private string categoryName;
        private decimal intakePrice;
        private int inventory;
        private int categoryID;
        private string foodName;
        private decimal price;
        private byte[] image;
        private int count;
        private decimal cost;
        public int FoodID { get => foodID; set => foodID = value; }
        public string CategoryName { get => categoryName; set => categoryName = value; }
        public decimal IntakePrice { get => intakePrice; set => intakePrice = value; }
        public int Inventory { get => inventory; set => inventory = value; }
        public int CategoryID { get => categoryID; set => categoryID = value; }
        public string FoodName { get => foodName; set => foodName = value; }
        public decimal Price { get => price; set => price = value; }
        public byte[] Image { get => image; set => image = value; }
        public int Count { get => count; set => count = value; }
        public decimal Cost { get => cost; set => cost = value; }
    }

    public class FoodOnCom
    {
        public FoodOnCom(DataRow row)
        {
            this.FoodName = row["foodname"].ToString();
            this.Count = (int)row["count"];
            this.Price = (decimal)row["price"];
            this.Cost = (decimal)row["cost"];
        }

        private decimal cost;
        private decimal price;
        private int count;
        private string foodName;

        public string FoodName { get => foodName; set => foodName = value; }
        public int Count { get => count; set => count = value; }
        public decimal Price { get => price; set => price = value; }
        public decimal Cost { get => cost; set => cost = value; }
    }
    public class Category
    {

        public string CategoryName { get; set; }
    }
}