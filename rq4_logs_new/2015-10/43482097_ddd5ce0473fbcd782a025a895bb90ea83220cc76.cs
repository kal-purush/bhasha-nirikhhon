using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Domain.Entities;

namespace Data.DumbData
{
    public static partial class Storage
    {
        public static List<Goods> goodsCollection = AddTenGoods();

        private static List<Goods> AddTenGoods()
        {
 	        List<Goods> goodsCollection = new List<Goods>();
            for (int i = 0; i < 9; i++)
            {
                Goods goods = new Goods();
                goods.Category = categoryCollection[i];
                goods.Count = i;
                goods.Coments.Add(new Comment() { Message = "GoodsComment" + i });
                goods.Name = "goodsName " + i;
                goods.Operator = userCollection[i];
                goods.Price = i * 1.11;
                goods.SKU = new String((char)i, 10);
            }
            return goodsCollection;
        }
    }  
}