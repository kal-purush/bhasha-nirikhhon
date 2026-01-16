using System;

namespace aweXpect.Chronology;

/// <summary>
///     Extension methods for creating <see cref="DateTime" />.
/// </summary>
/// <example>
///     Instead of <c>new DateTime(2024, 24, 12)</c><br />
///     you can write <c>24.December(2024)</c>.
/// </example>
public static class DateTimeExtensions
{
	/// <summary>
	///     Creates a <see cref="DateTime" /> for the given <paramref name="day" />
	///     in January of the given <paramref name="year" />.
	/// </summary>
	public static DateTime January(this int day, int year)
		=> new(year, 1, day, 0, 0, 0, DateTimeKind.Unspecified);

	/// <summary>
	///     Creates a <see cref="DateTime" /> for the given <paramref name="day" />
	///     in February of the given <paramref name="year" />.
	/// </summary>
	public static DateTime February(this int day, int year)
		=> new(year, 2, day, 0, 0, 0, DateTimeKind.Unspecified);

	/// <summary>
	///     Creates a <see cref="DateTime" /> for the given <paramref name="day" />
	///     in March of the given <paramref name="year" />.
	/// </summary>
	public static DateTime March(this int day, int year)
		=> new(year, 3, day, 0, 0, 0, DateTimeKind.Unspecified);

	/// <summary>
	///     Creates a <see cref="DateTime" /> for the given <paramref name="day" />
	///     in April of the given <paramref name="year" />.
	/// </summary>
	public static DateTime April(this int day, int year)
		=> new(year, 4, day, 0, 0, 0, DateTimeKind.Unspecified);

	/// <summary>
	///     Creates a <see cref="DateTime" /> for the given <paramref name="day" />
	///     in May of the given <paramref name="year" />.
	/// </summary>
	public static DateTime May(this int day, int year)
		=> new(year, 5, day, 0, 0, 0, DateTimeKind.Unspecified);

	/// <summary>
	///     Creates a <see cref="DateTime" /> for the given <paramref name="day" />
	///     in June of the given <paramref name="year" />.
	/// </summary>
	public static DateTime June(this int day, int year)
		=> new(year, 6, day, 0, 0, 0, DateTimeKind.Unspecified);

	/// <summary>
	///     Creates a <see cref="DateTime" /> for the given <paramref name="day" />
	///     in July of the given <paramref name="year" />.
	/// </summary>
	public static DateTime July(this int day, int year)
		=> new(year, 7, day, 0, 0, 0, DateTimeKind.Unspecified);

	/// <summary>
	///     Creates a <see cref="DateTime" /> for the given <paramref name="day" />
	///     in August of the given <paramref name="year" />.
	/// </summary>
	public static DateTime August(this int day, int year)
		=> new(year, 8, day, 0, 0, 0, DateTimeKind.Unspecified);

	/// <summary>
	///     Creates a <see cref="DateTime" /> for the given <paramref name="day" />
	///     in September of the given <paramref name="year" />.
	/// </summary>
	public static DateTime September(this int day, int year)
		=> new(year, 9, day, 0, 0, 0, DateTimeKind.Unspecified);

	/// <summary>
	///     Creates a <see cref="DateTime" /> for the given <paramref name="day" />
	///     in October of the given <paramref name="year" />.
	/// </summary>
	public static DateTime October(this int day, int year)
		=> new(year, 10, day, 0, 0, 0, DateTimeKind.Unspecified);

	/// <summary>
	///     Creates a <see cref="DateTime" /> for the given <paramref name="day" />
	///     in November of the given <paramref name="year" />.
	/// </summary>
	public static DateTime November(this int day, int year)
		=> new(year, 11, day, 0, 0, 0, DateTimeKind.Unspecified);

	/// <summary>
	///     Creates a <see cref="DateTime" /> for the given <paramref name="day" />
	///     in December of the given <paramref name="year" />.
	/// </summary>
	public static DateTime December(this int day, int year)
		=> new(year, 12, day, 0, 0, 0, DateTimeKind.Unspecified);


	/// <summary>
	///     Creates a <see cref="DateTime" /> for the given <paramref name="date" /> and <paramref name="time" />.
	/// </summary>
	public static DateTime At(this DateTime date, TimeSpan time)
		=> date.Date + time;

	/// <summary>
	///     Creates a <see cref="DateTime" /> for the given <paramref name="date" /> and time
	///     with the specified <paramref name="hours" />, <paramref name="minutes" />
	///     and optionally <paramref name="seconds" /> and <paramref name="milliseconds" />.
	/// </summary>
	public static DateTime At(this DateTime date, int hours, int minutes, int seconds = 0, int milliseconds = 0)
		=> new(date.Year, date.Month, date.Day, hours, minutes, seconds, milliseconds, date.Kind);


	/// <summary>
	///     Creates a <see cref="DateTime" /> for the given <paramref name="dateTime" />
	///     with kind set to <see cref="DateTimeKind.Utc" />.
	/// </summary>
	public static DateTime AsUtc(this DateTime dateTime)
		=> DateTime.SpecifyKind(dateTime, DateTimeKind.Utc);

	/// <summary>
	///     Creates a <see cref="DateTime" /> for the given <paramref name="dateTime" />
	///     with kind set to <see cref="DateTimeKind.Local" />.
	/// </summary>
	public static DateTime AsLocal(this DateTime dateTime)
		=> DateTime.SpecifyKind(dateTime, DateTimeKind.Local);


	/// <summary>
	///     Creates a <see cref="DateTime" /> that is the <paramref name="timeDifference" /> before the
	///     given <paramref name="dateTime" />.
	/// </summary>
	public static DateTime Before(this TimeSpan timeDifference, DateTime dateTime)
		=> dateTime - timeDifference;

	/// <summary>
	///     Creates a <see cref="DateTime" /> that is the <paramref name="timeDifference" /> after the
	///     specified <paramref name="dateTime" />.
	/// </summary>
	public static DateTime After(this TimeSpan timeDifference, DateTime dateTime)
		=> dateTime + timeDifference;
}