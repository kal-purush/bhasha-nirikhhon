namespace SpecFlowPluginBase.Exceptions
{
    using System;
    using System.Runtime.Serialization;

    [Serializable]
    public class WrongContainerSetupSignatureException : Exception
    {
        public WrongContainerSetupSignatureException()
        {
        }

        public WrongContainerSetupSignatureException(string message)
            : base(message)
        {
        }

        public WrongContainerSetupSignatureException(string message, Exception inner)
            : base(message, inner)
        {
        }

        protected WrongContainerSetupSignatureException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}