using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace ProjectManagementSystem.BusinessLogicLayer.Exceptions
{
    public class ClosedAccessException : Exception
    {
        public ClosedAccessException()
        {
        }

        public ClosedAccessException(string message) : base(message)
        {
        }

        public ClosedAccessException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected ClosedAccessException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}