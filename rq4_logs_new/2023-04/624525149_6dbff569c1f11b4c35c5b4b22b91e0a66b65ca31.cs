using MediatR;
using Microsoft.AspNetCore.Identity;
using TG.Backend.Services;

namespace TG.Backend.Features.Handler
{
    public class ResetPasswordHandler : IRequestHandler<ResetPasswordCommand, AuthResponseModel>
    {
        private readonly UserManager<AppUser> _userManager;
        private readonly ISendPasswordTokenService _sendPasswordTokenService;

        public ResetPasswordHandler(UserManager<AppUser> userManager, ISendPasswordTokenService sendPasswordTokenService)
        {
            _userManager = userManager;
            _sendPasswordTokenService = sendPasswordTokenService;
        }

        public async Task<AuthResponseModel> Handle(ResetPasswordCommand request, CancellationToken cancellationToken)
        {
            AppUser user = await _userManager.FindByEmailAsync(request.AppUser.Email);

            if (user is null)
            {
                return new()
                {
                    IsSuccess = false,
                    StatusCode = HttpStatusCode.BadRequest,
                    Messages = new[] { "Provided credentials are invalid" }
                };
            }

            string token = await _userManager.GeneratePasswordResetTokenAsync(user);

            await _sendPasswordTokenService.SendToken(user, token);

            return new()
            {
                IsSuccess = true,
                StatusCode = HttpStatusCode.NoContent,
                Messages = new string[] { }
            };
        }
    }
}