using System;
using System.Collections.Generic;
using System.Text;

namespace Lesson2._3
{
    class Invoice
    {
        readonly int account;
        readonly string customer;
        readonly string provider;

        public Invoice(int account, string customer, string provider)
        {
            this.account = account;
            this.customer = customer;
            this.provider = provider;
        }

        public string Article { get; set; }
        public int Quantity { get; set; }

        public double CostCalculate(bool ndsExistence)
        {
            double cost = 0;
            double costBook = 200;
            double costPen = 30;
            double nds = 0.2;

            switch (Article)
            {
                case "book":
                    cost = costBook * (double)Quantity;
                    break;
                case "pen":
                    cost = costPen * (double)Quantity;
                    break;
            }
            return cost = (ndsExistence == true) ? (cost * (1+nds)) : cost;
        }
    }
}