using System;
using System.Runtime.Serialization;

namespace Remoteit.Exceptions
{
    [Serializable]
    internal class RemoteitException : Exception
    {
        public RemoteitException()
        {
        }

        public RemoteitException(string message) : base(message)
        {
        }

        public RemoteitException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected RemoteitException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}