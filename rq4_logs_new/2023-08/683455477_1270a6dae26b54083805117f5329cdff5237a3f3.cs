using Codend.Domain.Core.Errors;
using FluentResults;

namespace Codend.Domain.Core.Extensions;

public static class ResultExtensions
{
    public static Result<T> Ensure<T>(this Result<T> result, Func<bool> predicate, DomainErrors.DomainError error)
    {
        try
        {
            if (!predicate())
            {
                return result;
            }
        }
        catch (Exception)
        {
            return result.WithError(error);
        }

        return result.WithError(error);
    }
}