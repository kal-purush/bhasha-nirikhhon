using System.Security.Claims;
using CSharpFunctionalExtensions;
using PetFamily.Accounts.Application.Models;
using PetFamily.Accounts.Domain.DataModels;
using PetFamily.SharedKernel.CustomErrors;

namespace PetFamily.Accounts.Application.Abstractions;

public interface ITokenProvider
{
     Task<JwtTokenResult> GenerateAccessToken(User user, CancellationToken cancellationToken);
     Task<Guid> GenerateRefreshToken(User user, Guid accessTokenJti, CancellationToken cancellationToken);
     Task<Result<IReadOnlyList<Claim>, Error>> GetUserClaims(string jwtToken);
}