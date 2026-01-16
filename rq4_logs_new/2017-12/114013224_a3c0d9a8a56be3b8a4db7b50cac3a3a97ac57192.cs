using PRN292_LAB_2.Entities;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Web;

namespace PRN292_LAB_2.Data
{
    public class CartUI
    {
        public static DataTable getDataTable(List<OrderDetail> list)
        {
            DataColumn[] cols = new DataColumn[] {
                new DataColumn("ID", typeof(int)),
                new DataColumn("Name", typeof(string)),
                new DataColumn("Price",typeof(string)),
                new DataColumn("Category",typeof(string)),
                new DataColumn("Quantity",typeof(string)),
            };

            DataTable dt = new DataTable();

            dt.Columns.AddRange(cols);
            foreach (OrderDetail order in list)
            {
                Product p = order.Product;
                dt.Rows.Add(p.id, p.name, p.price, p.category.name, order.quantity);
            }

            return dt;
        }
    }
}