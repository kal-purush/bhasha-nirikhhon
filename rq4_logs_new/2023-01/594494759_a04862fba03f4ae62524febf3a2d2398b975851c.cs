using System.Runtime.Serialization;

namespace MDLibrary.Exceptions;

public sealed class ApplicationErrorException : ApplicationException
{
    public ApplicationErrorException()
    {
    }

    public ApplicationErrorException(string message) : base(message)
    {
    }

    public ApplicationErrorException(SerializationInfo info, StreamingContext context) : base(info, context)
    {
    }

    public ApplicationErrorException(string message, Exception innerException) : base(message, innerException)
    {
    }
}