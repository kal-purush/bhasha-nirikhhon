using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime;
using System.Text;
using System.Threading.Tasks;

namespace Revision3
{
     public class loops
    {
        public void GetProductName()
        {
           IList<string> itemNames= new List<string>() {"spring water", "bread","nokia phone ","honey" };
            foreach (var itemName in itemNames)
            {
                Console.WriteLine(itemName);
            }
        }
    }
}