using CSharpFunctionalExtensions;
using Microsoft.Extensions.DependencyInjection;
using P2Project.Accounts.Agreements.Responses;
using P2Project.Accounts.Application.Interfaces;
using P2Project.Core;
using P2Project.Core.Interfaces;
using P2Project.Core.Interfaces.Commands;
using P2Project.Framework.Authorization;
using P2Project.SharedKernel.Errors;

namespace P2Project.Accounts.Application.Commands.RefreshTokens;

public class RefreshTokensHandler :
    ICommandHandler<LoginResponse, RefreshTokensCommand>
{
    private readonly IRefreshSessionManager _refreshSessionManager;
    private readonly ITokenProvider _tokenProvider;
    private readonly IUnitOfWork _unitOfWork;

    public RefreshTokensHandler(
        IRefreshSessionManager refreshSessionManager,
        ITokenProvider tokenProvider,
        [FromKeyedServices(Modules.Accounts)] IUnitOfWork unitOfWork)
    {
        _refreshSessionManager = refreshSessionManager;
        _tokenProvider = tokenProvider;
        _unitOfWork = unitOfWork;
    }

    public async Task<Result<LoginResponse, ErrorList>> Handle(
        RefreshTokensCommand command,
        CancellationToken cancellationToken)
    {
        var oldRefreshSession = await _refreshSessionManager
            .GetByRefreshToken(command.RefreshToken, cancellationToken);
        if(oldRefreshSession.IsFailure)
            return Errors.General.NotFound(command.RefreshToken).ToErrorList();
        
        if(oldRefreshSession.Value.ExpiresIn < DateTime.UtcNow)
            return Errors.AccountError.TokenExpired().ToErrorList();

        var userClaims = await _tokenProvider.GetUserClaims(command.AccessToken, cancellationToken);
        if (userClaims.IsFailure)
            return Errors.AccountError.InvalidToken().ToErrorList();
        
        var userIdString = userClaims.Value.FirstOrDefault(c => c.Type == CustomClaims.Id)?.Value;
        if(!Guid.TryParse(userIdString, out var userId))
            return Errors.General.Failure(userIdString).ToErrorList();
        if(oldRefreshSession.Value.UserId != userId)
            return Errors.AccountError.InvalidToken().ToErrorList();
        
        var userJtiString = userClaims.Value.FirstOrDefault(c => c.Type == CustomClaims.Jti)?.Value;
        if(!Guid.TryParse(userJtiString, out var userJtiGuid))
            return Errors.General.Failure(userJtiString).ToErrorList();
        if(oldRefreshSession.Value.Jti != userJtiGuid)
            return Errors.AccountError.InvalidToken().ToErrorList();

        _refreshSessionManager.Delete(oldRefreshSession.Value);
        await _unitOfWork.SaveChanges(cancellationToken);

        var accessToken = await _tokenProvider
            .GenerateAccessToken(oldRefreshSession.Value.User, cancellationToken);
        var refreshToken = await _tokenProvider
            .GenerateRefreshToken(oldRefreshSession.Value.User, accessToken.Jti, cancellationToken);
        
        return new LoginResponse(accessToken.AccessToken, refreshToken);
    }
}