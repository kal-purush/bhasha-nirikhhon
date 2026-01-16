using Soenneker.Cloudflare.Turnstile.Validator.Responses;
using Soenneker.Validators.Validator.Abstract;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.Cloudflare.Turnstile.Validator.Abstract;

/// <summary>
/// A validation module checking Cloudflare Turnstile tokens
/// </summary>
public interface ITurnstileValidator : IValidator
{
    /// <summary>
    /// Asynchronously gets the Turnstile validation response based on the provided token and optional remote IP address.
    /// </summary>
    /// <param name="token">The token to validate.</param>
    /// <param name="remoteIp">The optional remote IP address of the client.</param>
    /// <param name="cancellationToken">The cancellation token to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask{TResult}"/> that represents the asynchronous operation, containing a <see cref="TurnstileValidationResponse"/> if the operation succeeds, or <c>null</c> if it fails.</returns>
    ValueTask<TurnstileValidationResponse?> GetResponse(string token, string? remoteIp = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously validates the provided token with the optional remote IP address.
    /// </summary>
    /// <param name="token">The token to validate.</param>
    /// <param name="remoteIp">The optional remote IP address of the client.</param>
    /// <param name="cancellationToken">The cancellation token to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask{TResult}"/> that represents the asynchronous operation, containing <c>true</c> if the validation succeeds, or <c>false</c> if it fails.</returns>
    ValueTask<bool> Validate(string token, string? remoteIp = null, CancellationToken cancellationToken = default);
}