namespace Plugin.MvvmToolkit.Exceptions;

public sealed class DuplicateRouteException : Exception
{
    public DuplicateRouteException(string message) : base(message)
    {
    }
}