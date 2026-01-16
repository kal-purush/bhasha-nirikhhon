using MediatR;
using Microsoft.AspNetCore.Identity;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Localization;
using Microsoft.Extensions.Logging;
using MTAA_Backend.Application.Extensions;
using MTAA_Backend.Application.Identity.Commands;
using MTAA_Backend.Application.Identity.Queries;
using MTAA_Backend.Domain.DTOs.Users.Other;
using MTAA_Backend.Domain.DTOs.Users.Responses;
using MTAA_Backend.Domain.Entities.Users;
using MTAA_Backend.Domain.Exceptions;
using MTAA_Backend.Domain.Resources.Customers;
using MTAA_Backend.Domain.Resources.Localization.Errors;
using MTAA_Backend.Infrastructure;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace MTAA_Backend.Application.Identity.CommandHandlers
{
    public class SignUpByEmailHandler(IDistributedCache distributedCache,
        ILogger<SignUpByEmailHandler> logger,
        UserManager<User> userManager,
        RoleManager<IdentityRole> roleManager,
        IStringLocalizer<ErrorMessages> localizer,
        IMediator mediator,
        MTAA_BackendDbContext dbContext) : IRequestHandler<SignUpByEmail, TokenDTO>
    {
        private readonly IDistributedCache _distributedCache = distributedCache;
        private readonly ILogger _logger = logger;
        private readonly UserManager<User> _userManager = userManager;
        private readonly RoleManager<IdentityRole> _roleManager = roleManager;
        private readonly IStringLocalizer<ErrorMessages> _localizer = localizer;
        private readonly IMediator _mediator = mediator;
        private readonly MTAA_BackendDbContext _dbContext = dbContext;

        public async Task<TokenDTO> Handle(SignUpByEmail request, CancellationToken cancellationToken)
        {
            var oldUser = await _dbContext.Users.FirstOrDefaultAsync(u => u.Email == request.Email, cancellationToken);
            if (oldUser != null)
            {
                throw new HttpException(_localizer[ErrorMessagesPatterns.EmailAlreadyExists], HttpStatusCode.BadRequest);
            }

            var recordId = "EmailVerificationCode_" + request.Email;
            var codeModel = await _distributedCache.GetRecordAsync<VerificationCodeModel>(recordId);
            if (codeModel == null)
            {
                _logger.LogError($"Error while getting verification code from cache: {recordId}");
                throw new HttpException(_localizer[ErrorMessagesPatterns.SignUpSessionExpired], HttpStatusCode.BadRequest);
            }
            if (!codeModel.IsVerified)
            {
                _logger.LogError($"Creating account while not verified {recordId}");
                throw new HttpException(_localizer[ErrorMessagesPatterns.EmailIsNotVerified], HttpStatusCode.BadRequest);
            }

            var user = new User()
            {
                Email = request.Email,
                UserName = request.UserName,
                EmailConfirmed = true,
                Status = ""
            };

            var result = await _userManager.CreateAsync(user, request.Password);
            if (!result.Succeeded)
            {
                _logger.LogError($"Error while creating user: {result.Errors}");
                throw new HttpException(_localizer[ErrorMessagesPatterns.UserCreationError], HttpStatusCode.BadRequest);
            }

            if (!await _roleManager.RoleExistsAsync(UserRoles.User))
                await _roleManager.CreateAsync(new IdentityRole(UserRoles.User));

            if (await _roleManager.RoleExistsAsync(UserRoles.User))
            {
                await _userManager.AddToRoleAsync(user, UserRoles.User);
            }
            else
            {
                _logger.LogError($"Error while creating role: {result.Errors}");
            }

            return await _mediator.Send(new LogIn()
            {
                Email = request.Email,
                Password = request.Password
            });
        }
    }
}