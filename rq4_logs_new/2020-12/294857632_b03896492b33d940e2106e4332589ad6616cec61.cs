using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace ApplicationCore.Exceptions
{
    public class ProviderNotResponseException : Exception
    {
        public ProviderNotResponseException():base("Proveedor {0}, {1} no disponible")
        {
        }

        public ProviderNotResponseException(string message) : base(message)
        {
        }

        public ProviderNotResponseException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected ProviderNotResponseException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}