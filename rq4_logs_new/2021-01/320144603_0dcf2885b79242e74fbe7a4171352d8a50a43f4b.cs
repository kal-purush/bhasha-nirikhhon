using System;
using System.Linq;
using Games.Eucre.Api.Enums;
using Games.Eucre.Api.Extensions;
using Xunit;

namespace Games.Eucre.Tests.Extensions
{
	public class PlayerNumberExtensionsTests
	{
		[Theory]
		[InlineData(PlayerNumber.Red1, PlayerNumber.Black1)]
		[InlineData(PlayerNumber.Black1, PlayerNumber.Red2)]
		[InlineData(PlayerNumber.Red2, PlayerNumber.Black2)]
		[InlineData(PlayerNumber.Black2, PlayerNumber.Red1)]
		public void NextPlayer_Explicit(PlayerNumber input, PlayerNumber expected)
		{
			var result = input.NextPlayer();

			Assert.Equal(expected, result);
		}
	}
}