using System;
using System.Runtime.Serialization;

namespace Common.Exceptions
{
    public class UserNotAddedException : Exception
    {
        public string ResourceReferenceProperty { get; set; }

        public UserNotAddedException() : base("Can`t add new user to database!")
        {

        }
        public UserNotAddedException(string message) : base(message)
        {

        }
        public UserNotAddedException(string message, Exception inner) : base(message, inner)
        {

        }
        protected UserNotAddedException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            ResourceReferenceProperty = info.GetString("ResourceReferenceProperty");
        }
    }
}