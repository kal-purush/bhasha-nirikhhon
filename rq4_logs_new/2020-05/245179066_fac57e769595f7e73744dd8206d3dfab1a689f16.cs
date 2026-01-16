using FluentAssertions;
using NewEggAccess.Shared;
using NUnit.Framework;
using System;

namespace NewEggTests
{
	[ TestFixture ]
	public class DateTimeConversionTests
	{
		private const int pacificUtcDiffDaylightSavings = -7;

		[ Test ]
		public void ConvertFromUtcToPstStr()
		{
			const int Year = 2020;
			const int Month = 5;
			const int Day = 6;
			const int Hour = 20;
			const int Minute = 2;
			var utcDate = new DateTime( Year, Month, Day, Hour, Minute, 1, DateTimeKind.Utc ); 

			var pacificDate = DateTime.Parse( Misc.ConvertFromUtcToPstStr( utcDate ) );

			pacificDate.Year.Should().Be( Year );
			pacificDate.Month.Should().Be( Month );
			pacificDate.Day.Should().Be( Day );
			pacificDate.Hour.Should().Be( Hour + pacificUtcDiffDaylightSavings );
			pacificDate.Minute.Should().Be( Minute );
		}

		[ Test ]
		public void ConvertFromUtcToPstStr_PacificAfterNoon()
		{
			const int Hour = 20;
			var utcAfter8PM = new DateTime( 2020, 5, 6, Hour, 0, 1, DateTimeKind.Utc ); 

			var pacificAfterNoon = Misc.ConvertFromUtcToPstStr( utcAfter8PM );

			DateTime.Parse( pacificAfterNoon ).Hour.Should().Be( Hour + pacificUtcDiffDaylightSavings );
		}

		[ Test ]
		public void ConvertFromUtcToPstStr_PacificAfterMidnight()
		{
			const int Hour = 7;
			var utcAfter7AM = new DateTime( 2020, 5, 6, Hour, 0, 1, DateTimeKind.Utc ); 

			var pacificAfterMidnight = Misc.ConvertFromUtcToPstStr( utcAfter7AM );

			DateTime.Parse( pacificAfterMidnight ).Hour.Should().Be( Hour + pacificUtcDiffDaylightSavings );
		}

		[ Test ]
		public void ConvertFromUtcToPstStr_PacificBeforeMidnightPrevDay()
		{
			const int Hour = 6;
			const int Day = 6;
			var utcAfter7AM = new DateTime( 2020, 5, Day, Hour, 0, 1, DateTimeKind.Utc ); 

			var pacificAfterMidnight = Misc.ConvertFromUtcToPstStr( utcAfter7AM );

			DateTime.Parse( pacificAfterMidnight ).Hour.Should().Be( Hour + pacificUtcDiffDaylightSavings + 24 );
			DateTime.Parse( pacificAfterMidnight ).Day.Should().Be( Day - 1);
		}
	}
}