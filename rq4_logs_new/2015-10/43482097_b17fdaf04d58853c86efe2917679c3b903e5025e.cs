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

        public static List<GoodsRow> goodsRowCollection = AddTenRows();

        private static List<GoodsRow> AddTenRows()
        {
            List<GoodsRow> gooddsRowCollection = new List<GoodsRow>();
            for (int i = 0; i < 9; i++)
            {
                gooddsRowCollection.Add(new GoodsRow(userCollection[i], goodsCollection[i], i*1.1));
            }
            return gooddsRowCollection;
        }

    }
}