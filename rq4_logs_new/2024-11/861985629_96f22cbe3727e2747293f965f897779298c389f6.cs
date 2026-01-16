using Airbnb.Application.Features.Notifications;
using Airbnb.Application.Features.PaymentBooking.Command.BookingCancelation;
using Airbnb.Application.Services;
using Airbnb.Domain.DataTransferObjects.User;
using Airbnb.Tests.FakeObjects;
using FluentAssertions;
using MediatR;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using Stripe;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Airbnb.Tests.AuthenticationTests
{
	public class RestePasswordTest:TestBase
	{
		[Fact]
		public async Task ResetPassword_WhenPasswordChangedSuccessfully_ShouldReturnSuccessResponse()
		{
			// Arrange
			var _userManager = GetUserManager();
			var user = new FakeAccount().Generate();
			await _userManager.CreateAsync(user,"Pa$$w0rd");

			var _memCach = _serviceProvider.GetRequiredService<IMemoryCache>();
			string Otp = "12345";
			_memCach.Set(user.Email!,Otp);

			var token = await _userManager.GeneratePasswordResetTokenAsync(user);
			token.Should().NotBeNullOrEmpty();
			var request = new ResetPasswordDTO
			{
				Email = user.Email!,
				Otp = Otp,
				NewPassword = "Test@123",
				PasswordComfirmation = "Test@123",
				Token = token
			};
			var _mediator = new Mock<IMediator>();
			_mediator.Setup(x => x.Publish(It.IsAny<NotificationEvent>(), It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
			var handler = new UserService(_userManager, null, null, null, null, _memCach, null,_mediator.Object,null);

			// Act
			var result = await handler.ResetPassword(request);

			//Assert
			result.IsSuccess.Should().BeTrue();
			var IsUpdatd =await _userManager.CheckPasswordAsync(user,"Test@123");
			IsUpdatd.Should().BeTrue();
		}

		[Fact]
		public async Task ResetPassword_WhenUserNotFound_ShouldReturnFailureResponse()
		{
			// Arrange
			var _userManager = GetUserManager();
			var request = new ResetPasswordDTO
			{
				Email = "notfound@gmail.com",
				Otp = "12345",
				NewPassword = "Test@123",
				PasswordComfirmation = "Test@123",
				Token = "invalidToken"
			};

			var _memCach = _serviceProvider.GetRequiredService<IMemoryCache>();
			var _mediator = new Mock<IMediator>();
			var handler = new UserService(_userManager, null, null, null, null, _memCach, null, _mediator.Object, null);

			// Act
			var result = await handler.ResetPassword(request);

			// Assert
			result.IsSuccess.Should().BeFalse();
			result.Message.Should().Be("Email is not found!");
		}

		[Fact]
		public async Task ResetPassword_WhenOtpExpired_ShouldReturnFailureResponse()
		{
			// Arrange
			var _userManager = GetUserManager();
			var user = new FakeAccount().Generate();
			await _userManager.CreateAsync(user, "Pa$$w0rd");

			var request = new ResetPasswordDTO
			{
				Email = user.Email!,
				Otp = "12345",
				NewPassword = "Test@123",
				PasswordComfirmation = "Test@123",
				Token = "invalidToken"
			};

			var _mediator = new Mock<IMediator>();
			var handler = new UserService(_userManager, null, null, null, null, new MemoryCache(new MemoryCacheOptions()), null, _mediator.Object, null);

			// Act
			var result = await handler.ResetPassword(request);

			// Assert
			result.IsSuccess.Should().BeFalse();
		}

		[Fact]
		public async Task ResetPassword_WhenOtpIsInvalid_ShouldReturnFailureResponse()
		{
			// Arrange
			var _userManager = GetUserManager();
			var user = new FakeAccount().Generate();
			await _userManager.CreateAsync(user, "Pa$$w0rd");

			var _memCach = _serviceProvider.GetRequiredService<IMemoryCache>();
			string validOtp = "12345";
			_memCach.Set(user.Email!, validOtp);

			var token = await _userManager.GeneratePasswordResetTokenAsync(user);
			var request = new ResetPasswordDTO
			{
				Email = user.Email!,
				Otp = "54321", 
				NewPassword = "Test@123",
				PasswordComfirmation = "Test@123",
				Token = token
			};

			var _mediator = new Mock<IMediator>();
			var handler = new UserService(_userManager, null, null, null, null, _memCach, null, _mediator.Object, null);

			// Act
			var result = await handler.ResetPassword(request);

			// Assert
			result.IsSuccess.Should().BeFalse();
		}

		[Fact]
		public async Task ResetPassword_WhenTokenIsInvalid_ShouldReturnFailureResponse()
		{
			// Arrange
			var _userManager = GetUserManager();
			var user = new FakeAccount().Generate();
			await _userManager.CreateAsync(user, "Pa$$w0rd");

			var _memCach = _serviceProvider.GetRequiredService<IMemoryCache>();
			string Otp = "12345";
			_memCach.Set(user.Email!, Otp);

			var invalidToken = "InvalidResetToken"; 
			var request = new ResetPasswordDTO
			{
				Email = user.Email!,
				Otp = Otp,
				NewPassword = "Test@123",
				PasswordComfirmation = "Test@123",
				Token = invalidToken
			};

			var _mediator = new Mock<IMediator>();
			var handler = new UserService(_userManager, null, null, null, null, _memCach, null, _mediator.Object, null);

			// Act
			var result = await handler.ResetPassword(request);

			// Assert
			result.IsSuccess.Should().BeFalse();
		}

		[Fact]
		public async Task ResetPassword_WhenPasswordValidationFails_ShouldReturnFailureResponse()
		{
			// Arrange
			var _userManager = GetUserManager();
			var user = new FakeAccount().Generate();
			await _userManager.CreateAsync(user, "Pa$$w0rd");

			var _memCach = _serviceProvider.GetRequiredService<IMemoryCache>();
			string Otp = "12345";
			_memCach.Set(user.Email!, Otp);

			var token = await _userManager.GeneratePasswordResetTokenAsync(user);
			var request = new ResetPasswordDTO
			{
				Email = user.Email!,
				Otp = Otp,
				NewPassword = "short", 
				PasswordComfirmation = "short",
				Token = token
			};

			var _mediator = new Mock<IMediator>();
			var handler = new UserService(_userManager, null, null, null, null, _memCach, null, _mediator.Object, null);

			// Act
			var result = await handler.ResetPassword(request);

			// Assert
			result.IsSuccess.Should().BeFalse();
		}

	}
}