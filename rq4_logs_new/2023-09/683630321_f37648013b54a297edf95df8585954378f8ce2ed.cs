using Application.Helpers;
using Application.Repository;
using Common.Dtos;
using MediatR;
using Microsoft.Extensions.Logging;

namespace Application.Handlers.ApplicationUsers
{
    public class ChangeUserPasswordHandler : IRequestHandler<ChangeUserPasswordCommand, LogoutResponseDto>
    {
        private readonly ILogger<ChangeUserPasswordHandler> _logger;
        private readonly IApplicationUserRepository _repository;
        private readonly IHashHelper _hashHelper;

        public ChangeUserPasswordHandler(ILogger<ChangeUserPasswordHandler> logger, IApplicationUserRepository repository, IHashHelper hashHelper)
        {
            _logger = logger;
            _repository = repository;
            _hashHelper = hashHelper;
        }

        public async Task<LogoutResponseDto> Handle(ChangeUserPasswordCommand request, CancellationToken cancellationToken)
        {
            var errorMessage = "Old password do not match.";
            LogoutResponseDto response = new LogoutResponseDto();
            var user = _repository.Find(u => u.UserGuid == request.UserId).FirstOrDefault();
            if (user == null)
            {
                response.SetError(errorMessage);
                _logger.LogError(errorMessage);
            }
            else
            {
                if (!_hashHelper.VerifyPassword(request.OldPassword, user.PasswordEncrypted))
                {
                    response.SetError(errorMessage);
                    _logger.LogError(errorMessage);
                }
                else
                {
                    _logger.LogInformation("Generating tokens");
                    var newPassword = _hashHelper.HashPassword(request.NewPassword, user.PasswordSalt);
                    user.SetPassword(newPassword);
                    await _repository.UpdateAsync(user);
                    response.SetSuccess();
                    response.Message = "Password changed successfully";
                }
            }
            return response;
        }
    }
}