using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace Common.Exceptions
{
    public class PresetNotFoundException :Exception
    {
        public string ResourceReferenceProperty { get; set; }

        public PresetNotFoundException() : base("Preset does not Exist!")
        {

        }
        public PresetNotFoundException(string message) : base(message)
        {

        }
        public PresetNotFoundException(string message, Exception inner) : base(message, inner)
        {

        }
        protected PresetNotFoundException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            ResourceReferenceProperty = info.GetString("ResourceReferenceProperty");
        }
    }
}