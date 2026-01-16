using Airbnb.Application.Services;
using Airbnb.Tests.FakeObjects;
using FluentAssertions;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Airbnb.Tests.AuthenticationTests
{
	public class ConfirmEmailTest:TestBase
	{
		[Fact]
		public async Task ConfirmEamil_WhenEmailConfirmedSuccessflly_ShouldBeReturnSuccessResponse()
		{
			//Arrange
			var user = new FakeAccount().Generate();
			var _userManager = GetUserManager();
			await _userManager.CreateAsync(user);

			var email = user.Email;
			var Otp = await _userManager.GenerateEmailConfirmationTokenAsync(user);

			var _memoryCach = _serviceProvider.GetRequiredService<IMemoryCache>();
			_memoryCach.Set("Otp", Otp);

			var handler = new UserService(_userManager, null, null, null, null, _memoryCach, null, null, null);

			// Act
			var result = await handler.EmailConfirmation(email, Otp);

			//Assert
			result.IsSuccess.Should().BeTrue();

		}

		[Fact]
		public async Task EmailConfirmation_WhenEmailDoesNotExist_ShouldReturnFailureResponse()
		{
			// Arrange
			var _memoryCach = _serviceProvider.GetRequiredService<IMemoryCache>();
			var handler = new UserService(GetUserManager(), null, null, null, null,_memoryCach, null, null, null);

			// Act
			var result = await handler.EmailConfirmation("nonexistent@example.com", "12345");

			// Assert
			result.IsSuccess.Should().BeFalse();
			result.StatusCode.Should().Be(HttpStatusCode.NotFound);
		}

		[Fact]
		public async Task EmailConfirmation_WhenEmailOrCodeIsNull_ShouldReturnFailureResponse()
		{
			// Arrange
			var _memoryCach = _serviceProvider.GetRequiredService<IMemoryCache>();
			var handler = new UserService(GetUserManager(), null, null, null, null,_memoryCach, null, null, null);

			// Act
			var result = await handler.EmailConfirmation(null, null);

			// Assert
			result.IsSuccess.Should().BeFalse();
			result.Message.Should().Be("invalid payloads");
		}
		
		[Fact]
		public async Task EmailConfirmation_WhenOtpIsInvalid_ShouldReturnFailureResponse()
		{
			// Arrange
			var user = new FakeAccount().Generate();
			var _userManager = GetUserManager();
			await _userManager.CreateAsync(user);

			var email = user.Email;
			var validOtp = await _userManager.GenerateEmailConfirmationTokenAsync(user);

			var _memoryCache = _serviceProvider.GetRequiredService<IMemoryCache>();
			_memoryCache.Set("Otp", "InvalidOtp");

			var handler = new UserService(_userManager, null, null, null, null, _memoryCache, null, null, null);

			// Act
			var result = await handler.EmailConfirmation(email, validOtp);

			// Assert
			result.IsSuccess.Should().BeFalse();
			result.StatusCode.Should().Be(HttpStatusCode.InternalServerError);
		}
		
		[Fact]
		public async Task EmailConfirmation_WhenConfirmEmailFails_ShouldReturnFailureResponse()
		{
			// Arrange
			var user = new FakeAccount().Generate();
			var _userManager = GetUserManager();
			await _userManager.CreateAsync(user);

			var email = user.Email;
			var invalidOtp = "InvalidCode";

			var _memoryCache = _serviceProvider.GetRequiredService<IMemoryCache>();
			_memoryCache.Set("Otp", invalidOtp);

			var handler = new UserService(_userManager, null, null, null, null, _memoryCache, null, null, null);

			// Act
			var result = await handler.EmailConfirmation(email, invalidOtp);

			// Assert
			result.IsSuccess.Should().BeFalse();
		}

		[Fact]
		public async Task EmailConfirmation_WhenOtpIsMissingInCache_ShouldReturnFailureResponse()
		{
			// Arrange
			var user = new FakeAccount().Generate();
			var _userManager = GetUserManager();
			await _userManager.CreateAsync(user);

			var email = user.Email;
			var Otp = await _userManager.GenerateEmailConfirmationTokenAsync(user);

			var _memoryCache = _serviceProvider.GetRequiredService<IMemoryCache>();

			var handler = new UserService(_userManager, null, null, null, null, _memoryCache, null, null, null);

			// Act
			var result = await handler.EmailConfirmation(email, Otp);

			// Assert
			result.IsSuccess.Should().BeFalse();
			result.StatusCode.Should().Be(HttpStatusCode.InternalServerError);
		}

	}
}