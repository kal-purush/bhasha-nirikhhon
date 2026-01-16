namespace TG.Backend.Features.Handler
{
    public class ConfirmAccountHandler : IRequestHandler<ConfirmAccountCommand, AuthResponseModel>
    {
        private readonly UserManager<AppUser> _userManager;

        public ConfirmAccountHandler(UserManager<AppUser> userManager)
        {
            _userManager = userManager;
        }

        public async Task<AuthResponseModel> Handle(ConfirmAccountCommand request, CancellationToken cancellationToken)
        {
            (string token, string email) = request;

            AppUser user = await _userManager.FindByEmailAsync(email);

            if (user is null)
            {
                return new()
                {
                    IsSuccess = false,
                    StatusCode = HttpStatusCode.BadRequest,
                    Messages = new[] { "Provided credentials are invalid" }
                };
            }

            IdentityResult res = await _userManager.ConfirmEmailAsync(user, token);

            if (!res.Succeeded)
            {
                return new()
                {
                    IsSuccess = false,
                    StatusCode = HttpStatusCode.BadRequest,
                    Messages = res.Errors.Select(e => e.Description).ToArray()
                };
            }

            return new()
            {
                IsSuccess = true,
                StatusCode = HttpStatusCode.NoContent,
                Messages = new string[] { }
            };
        }
    }
}