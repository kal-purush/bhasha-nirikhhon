using MediatR;
using Microsoft.AspNetCore.Identity;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Application.User.UserCommands.CreateUser
{
        public class CreateUserCommandHandler : IRequestHandler<CreateUserCommand, IdentityResult>
        {
            private readonly UserManager<IdentityUser> _userManager;
            private readonly ILogger<CreateUserCommandHandler> _logger;

            public CreateUserCommandHandler(UserManager<IdentityUser> userManager, ILogger<CreateUserCommandHandler> logger)
            {
                _userManager = userManager;
                _logger = logger;
            }

            public async Task<IdentityResult> Handle(CreateUserCommand request, CancellationToken cancellationToken)
            {
                _logger.LogInformation("Attempting to create a new user with username: {Username}", request.Username);

                var user = new IdentityUser
                {
                    UserName = request.Username,
                    Email = request.Email
                };

                var result = await _userManager.CreateAsync(user, request.Password);

                if (result.Succeeded)
                {
                    _logger.LogInformation("User created successfully: {Username}", request.Username);
                }
                else
                {
                    _logger.LogError("Failed to create user: {Username}. Errors: {Errors}",
                        request.Username, string.Join(", ", result.Errors));
                }

                return result;
            }
        }
    }

