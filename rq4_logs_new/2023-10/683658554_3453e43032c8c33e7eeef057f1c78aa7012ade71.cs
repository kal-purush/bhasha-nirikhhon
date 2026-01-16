
using MediatR;
using Microsoft.AspNetCore.Identity;
using System.Security.Claims;
using YourScheduler.BusinessLogic.Models;
using YourScheduler.BusinessLogic.Models.DTOs;
using YourScheduler.BusinessLogic.Services.Interfaces;
using YourScheduler.Infrastructure.Entities;


namespace YourScheduler.BusinessLogic.Commands.AuthorizeUser
{
    public class AuthorizeUserCommandHandler : IRequestHandler<AuthorizeUserCommand, AuthorizationResponse>
    {
        private readonly SignInManager<ApplicationUser> _signInManager;
        private readonly UserManager<ApplicationUser> _userManager;
        private readonly IJwtTokenGenerator _jwtGenerator;

        public AuthorizeUserCommandHandler(SignInManager<ApplicationUser> signInManager, UserManager<ApplicationUser> userManager, IJwtTokenGenerator jwtGenerator)
        {
            _signInManager = signInManager;
            _userManager = userManager;
            _jwtGenerator = jwtGenerator;
        }

        public async Task<AuthorizationResponse> Handle(AuthorizeUserCommand request, CancellationToken cancellationToken)
        {
            int tokenExpiresInDays = 2;
            var user = await _userManager.FindByNameAsync(request.AuthorizationRequest.Email) ?? throw new Exception("User has been successfully sign in but couldn't be found. Identity mistake");
            var result = await _signInManager.CheckPasswordSignInAsync(user, request.AuthorizationRequest.Password, false);

            if(!result.Succeeded)
                throw new Exception("Invalid Email or Password");

            
            var token = _jwtGenerator.GenerateToken(new List<Claim>
            {
                new Claim(ClaimTypes.NameIdentifier, user.Id.ToString()),
                new Claim(ClaimTypes.Email, user.Email ?? throw new Exception("Wrong Email to generate Token")),
                new Claim(ClaimTypes.GivenName, user.Displayname)
            }, tokenExpiresInDays);

            return new AuthorizationResponse(user, token);

        }
    }
}