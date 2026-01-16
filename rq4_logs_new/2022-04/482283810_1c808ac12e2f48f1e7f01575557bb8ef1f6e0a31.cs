using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace com.mobuquity.packer
{
    public class APIException:Exception
    {
        public APIException()
        {
            Console.WriteLine("Something went wrong;");
        }

        public APIException(string message)
        {
            Console.WriteLine(message);
        }
        public APIException(Exception ex)
        {
            Console.WriteLine(ex);
        }
    }
}