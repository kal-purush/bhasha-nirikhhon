using System;

namespace aweXpect.Chronology;

#if NET8_0_OR_GREATER
/// <summary>
///     A builder that allows implicit cast to <see cref="DateTime" />, <see cref="DateOnly" />
///     and <see cref="TimeOnly" />.
/// </summary>
#else
/// <summary>
///     A builder that allows implicit cast to <see cref="DateTime" />.
/// </summary>
#endif
public readonly struct DateBuilder(DateTime value)
{
	private readonly DateTime _value = value;

	/// <summary>
	///     Implicitly casts the <see cref="DateBuilder" /> to a <see cref="DateTime" />.
	/// </summary>
	public static implicit operator DateTime(DateBuilder builder)
		=> builder._value;

#if NET8_0_OR_GREATER
	/// <summary>
	///     Implicitly casts the <see cref="DateBuilder" /> to a <see cref="DateOnly" />.
	/// </summary>
	public static implicit operator DateOnly(DateBuilder builder)
		=> DateOnly.FromDateTime(builder._value);
#endif

	/// <summary>
	///     Adds a <see cref="TimeSpan" /> to the <see cref="DateTime" />.
	/// </summary>
	public static DateBuilder operator +(DateBuilder builder, TimeSpan time)
		=> new(builder._value + time);

	/// <summary>
	///     Subtracts a <see cref="TimeSpan" /> from the <see cref="DateTime" />.
	/// </summary>
	public static DateBuilder operator -(DateBuilder builder, TimeSpan time)
		=> new(builder._value - time);

	internal DateTimeBuilder Add(TimeSpan time)
		=> new(_value + time);
}