using System;
using System.Runtime.Serialization;

namespace Common.Exceptions
{
    public class TaskNotFoundException : Exception
    {
        public string ResourceReferenceProperty { get; set; }

        public TaskNotFoundException() : base("Task does not exist!")
        {

        }
        public TaskNotFoundException(string message) : base(message)
        {

        }
        public TaskNotFoundException(string message, Exception inner) : base(message, inner)
        {

        }
        protected TaskNotFoundException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            ResourceReferenceProperty = info.GetString("ResourceReferenceProperty");
        }
    }
}