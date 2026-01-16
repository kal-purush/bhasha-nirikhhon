using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace prjVatAdd
{
    class Cart
    {
        List<double> price = new List<double>();
        String[] itemsString = new string[26];
        double vat;

        public Cart (List<double>price, String[] itemsString, double vat)
        {
            this.price = price;
            this.itemsString = itemsString;
            this.vat = vat;
        }


        public double getTotalPrice()
        {
            double total = 0;
            for (int i = 0; i < price.Count(); i++)
            {
                total = total + price[i];
            }

            return total;
        }


        public override string ToString()
        {
            String display = "";

            for (int i = 0; i < price.Count(); i++)
            {
                display += "Item " + itemsString[i] + " R " + price[i] + "\n";
            }

            display += "-------------------------" + "\n" +
            "TOTAL R " + getTotalPrice() + "\n" +
            "+ VAT R " + vat + "\n" +
            "-------------------------" + "\n" +
            "Grand Total: R " + (getTotalPrice()+vat)+
            "\n-------------------------" + "\n";


            return display;
        }
    }
}