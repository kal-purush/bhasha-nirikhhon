using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DesignPatternPartThree.Structural.Adapter
{
    public class ClientCodeForUsingAdaptor
    {
        public static void UsedAdapter()
        {
            string[,] employeesArray = new string[5, 2]
            {
                
                { "poly","500000"},
                { "Rajit","60000"},
                { "Adil","700000"},
                { "Labeeb","80000"},
                { "Mainul","90000"},
                
            };
        }
    }
}