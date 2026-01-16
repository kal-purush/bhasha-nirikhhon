using install.Interfaces.Basic;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace install.Basic.ModelsParts.Types.ResultTypes
{
    class ConnectionNotReadyType : ResultInterface
    {
        public string getType()
        {
            return "ConnectionNotReady";
        }
    }
}