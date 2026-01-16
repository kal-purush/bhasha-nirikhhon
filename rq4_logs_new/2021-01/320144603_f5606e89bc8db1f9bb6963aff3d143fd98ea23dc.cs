using System;
using Games.Eucre.Api.Data;
using Games.Eucre.Api.Data.Models;
using Games.Eucre.Api.Enums;
using Xunit;

namespace Games.Eucre.Tests.Unit.Data
{
	public static class ExtensionsTests
	{
		[Fact]
		public static void Player_IsBot_False()
		{
			Player player = new Player(
				PlayerNumber.Black1,
				new User("testId"),
				Array.Empty<Card>());

			Assert.False(player.IsBot());
		}

		[Fact]
		public static void Player_IsBot_True()
		{
			Player player = new Player(
				PlayerNumber.Black1,
				null,
				Array.Empty<Card>());

			Assert.True(player.IsBot());
		}
	}
}