using System;
using System.Runtime.Serialization;

namespace Remoteit.RestApi
{
    [Serializable]
    internal class RemoteitClientOperationException : Exception
    {
        public RemoteitClientOperationException()
        {
        }

        public RemoteitClientOperationException(string message) : base(message)
        {
        }

        public RemoteitClientOperationException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected RemoteitClientOperationException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}