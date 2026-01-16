using System;
using System.Linq;
using System.Threading.Tasks;
using IdentityServer4.Events;
using IdentityServer4.Models;
using IdentityServer4.Services;
using Losol.Communication.Sms;
using Losol.Identity.Model;
using Losol.Identity.Services.Util;
using Microsoft.AspNetCore.Identity;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace Losol.Identity.Services.Auth
{
    public class PhoneAuthenticationService : IPhoneAuthenticationService
    {
        private readonly ISmsSender _smsService;
        private readonly PhoneNumberTokenProvider<ApplicationUser> _phoneNumberTokenProvider;
        private readonly UserManager<ApplicationUser> _userManager;
        private readonly ILogger<PhoneAuthenticationService> _logger;
        private readonly IEventService _events;
        private readonly SignInManager<ApplicationUser> _signInManager;

        public PhoneAuthenticationService(
            ISmsSender smsService,
            PhoneNumberTokenProvider<ApplicationUser> phoneNumberTokenProvider,
            UserManager<ApplicationUser> userManager,
            ILogger<PhoneAuthenticationService> logger,
            IEventService events,
            SignInManager<ApplicationUser> signInManager)
        {
            _smsService = smsService;
            _phoneNumberTokenProvider = phoneNumberTokenProvider;
            _userManager = userManager;
            _logger = logger;
            _events = events;
            _signInManager = signInManager;
        }

        public async Task<ApplicationUser> SendVerificationCodeAsync(string phoneNumber)
        {
            if (string.IsNullOrEmpty(phoneNumber))
            {
                throw new ArgumentException(nameof(phoneNumber));
            }

            var user = await GetUserByPhoneAsync(phoneNumber);

            var verifyToken = await _phoneNumberTokenProvider
                .GenerateAsync(PhoneNumberVerificationPurpose, _userManager, user);

            await SendVerificationCodeAsync(phoneNumber, verifyToken);
            return user;
        }

        public async Task<ApplicationUser> AuthenticateAsync(
            string phoneNumber,
            string verificationToken,
            bool createUserIfNotExists = false)
        {
            if (string.IsNullOrEmpty(phoneNumber))
            {
                throw new ArgumentException(nameof(phoneNumber));
            }

            if (string.IsNullOrEmpty(verificationToken))
            {
                throw new ArgumentException(nameof(verificationToken));
            }

            var user = await GetUserByPhoneAsync(phoneNumber);
            var userExists = user.Id != DummyUserId;

            var valid = await _phoneNumberTokenProvider
                .ValidateAsync(PhoneNumberVerificationPurpose, verificationToken, _userManager, user);

            if (!valid)
            {
                _logger.LogInformation("Authentication failed for token: {token}, reason: invalid token",
                    verificationToken);
                await _events.RaiseAsync(new UserLoginFailureEvent(verificationToken,
                    "invalid token or verification id", false));
                return null;
            }

            _logger.LogInformation("Phone number verified: {phoneNumber}", phoneNumber);
            await _events.RaiseAsync(new UserLoginSuccessEvent(phoneNumber, user.Id, phoneNumber, false));

            if (!userExists && createUserIfNotExists)
            {
                user.Id = Guid.NewGuid().ToString();
                user.PhoneNumberConfirmed = true;
                var result = await _userManager.CreateAsync(user);
                if (result != IdentityResult.Success)
                {
                    var reason = result.Errors.Select(x => x.Description)
                        .Aggregate((a, b) => a + ", " + b);
                    _logger.LogInformation("User creation failed: {username}, reason: {reason}",
                        phoneNumber, reason);
                    await _events.RaiseAsync(new UserLoginFailureEvent(phoneNumber,
                        reason, false));
                    return null;
                }

                userExists = true;
            }

            if (!userExists)
            {
                return null;
            }

            await _signInManager.SignInAsync(user, true);
            return user;
        }

        public async Task<ApplicationUser> GetUserByPhoneAsync(string phoneNumber)
        {
            if (string.IsNullOrEmpty(phoneNumber))
            {
                throw new ArgumentException(nameof(phoneNumber));
            }

            phoneNumber = PhoneNumberUtil.NormalizePhoneNumber(phoneNumber);
            return await _userManager.Users.SingleOrDefaultAsync(x => x.PhoneNumber == phoneNumber)
                   ?? CreateDummyUser(phoneNumber);
        }

        private static ApplicationUser CreateDummyUser(string phoneNumber)
        {
            return new ApplicationUser
            {
                Id = DummyUserId,
                UserName = phoneNumber,
                PhoneNumber = phoneNumber,
                SecurityStamp = phoneNumber.Sha256()
            };
        }

        private async Task SendVerificationCodeAsync(string phoneNumber, string verificationCode)
        {
            // TODO: use message queue for this
            // TODO: localize
            await _smsService.SendSmsAsync(phoneNumber, $"Your login verification code is: {verificationCode}");
        }

        private const string DummyUserId = "dummy-user-id";
        private const string PhoneNumberVerificationPurpose = "verify_number";
    }
}