using System.Runtime.Serialization;

namespace Fossa.API.Core.Messages;

[Serializable]
public class CrossTenantOutboundUnauthorizedAccessException : CrossTenantUnauthorizedAccessException
{
  public CrossTenantOutboundUnauthorizedAccessException()
  { }

  public CrossTenantOutboundUnauthorizedAccessException(string message) : base(message)
  {
  }

  public CrossTenantOutboundUnauthorizedAccessException(string message, Exception inner) : base(message, inner)
  {
  }

  protected CrossTenantOutboundUnauthorizedAccessException(
    SerializationInfo info,
    StreamingContext context) : base(info, context) { }
}