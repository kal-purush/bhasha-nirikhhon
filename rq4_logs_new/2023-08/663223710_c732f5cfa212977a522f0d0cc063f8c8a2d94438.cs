using System.Runtime.Serialization;

namespace Fossa.API.Core.Messages;

[Serializable]
public class CrossTenantUnauthorizedAccessException : Exception
{
  public CrossTenantUnauthorizedAccessException()
  { }

  public CrossTenantUnauthorizedAccessException(string message) : base(message)
  {
  }

  public CrossTenantUnauthorizedAccessException(string message, Exception inner) : base(message, inner)
  {
  }

  protected CrossTenantUnauthorizedAccessException(
    SerializationInfo info,
    StreamingContext context) : base(info, context) { }
}