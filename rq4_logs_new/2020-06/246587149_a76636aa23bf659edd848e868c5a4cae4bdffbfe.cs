using System;
using System.Runtime.Serialization;

namespace Solari.Oberon
{
    [Serializable]
    public class OberonException : Exception
    {
        public OberonException()
        {
        }

        public OberonException(string message) : base(message)
        {
        }

        public OberonException(string message, Exception inner) : base(message, inner)
        {
        }

        protected OberonException(SerializationInfo info,
                                  StreamingContext context) : base(info, context)
        {
        }
    }
}