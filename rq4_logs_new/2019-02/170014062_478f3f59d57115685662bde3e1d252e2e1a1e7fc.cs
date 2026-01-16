using System;
using System.Runtime.Serialization;

namespace HackedBrain.BotBuilder.State.Conditional
{
    [Serializable]
    public class NoStatePropertyAccessorCouldBeSelectedException : Exception
    {
        public NoStatePropertyAccessorCouldBeSelectedException(string message) : base(message) { }
        public NoStatePropertyAccessorCouldBeSelectedException(string message, Exception inner) : base(message, inner) { }
        protected NoStatePropertyAccessorCouldBeSelectedException(
          SerializationInfo info,
          StreamingContext context) : base(info, context) { }
    }
}