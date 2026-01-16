using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Advanced_Lesson_1_OOP.InternetShop
{
    public class WareHouse
    {
        public Article article;
        public List<Article> articles;

        public Article Article
        {
            get => default(Article);
            set
            {
            }
        }
    }

    public struct Article
    {
        public string nameOfArticle { get; set; }
        public int quantityOnWarehouse { get; set; }
        public decimal priceOfArticle { get; set; }

        public Article(string articleName, int warehouseQuantity, decimal price)
        {
            nameOfArticle = articleName;
            quantityOnWarehouse = warehouseQuantity;
            priceOfArticle = price;
        }

        //A.L1.P6/6*
        public static bool operator > (Article a1, Article a2)
        {
            return a1.priceOfArticle > a2.priceOfArticle;
        }
        public static bool operator < (Article a1, Article a2)
        {
            return a1.priceOfArticle < a2.priceOfArticle;
        }

        public static bool operator == (Article a1, Article a2)
        {
            return a1.priceOfArticle == a2.priceOfArticle;
        }
        public static bool operator != (Article a1, Article a2)
        {
            return a1.priceOfArticle != a2.priceOfArticle;
        }
    }
}