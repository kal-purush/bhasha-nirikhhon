using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Challenge.Atm.Application.Exceptions
{
    public class ApiCustomException: Exception
    {
        public ApiCustomException(string message) : base(message)
        {
            
        }
 
    }
}