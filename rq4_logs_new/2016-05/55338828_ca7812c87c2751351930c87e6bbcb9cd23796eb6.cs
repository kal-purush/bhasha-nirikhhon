using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimSharp.Core.Resources.StoreObjects
{
    public class OP
    {
        public OP(String value)
        {
            ID = value;
        }
        public String ID {  get;  set; }
       public override String ToString()
        {
            return ID.ToString();
        }
    }
}