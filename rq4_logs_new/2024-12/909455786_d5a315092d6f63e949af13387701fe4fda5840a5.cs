using System;

namespace aweXpect.Chronology;

#if NET8_0_OR_GREATER
/// <summary>
///     A builder that allows implicit cast to <see cref="DateTimeOffset" />, <see cref="DateTime" />,
///     <see cref="DateOnly" /> and <see cref="TimeOnly" />.
/// </summary>
#else
/// <summary>
///     A builder that allows implicit cast to <see cref="DateTimeOffset" /> and <see cref="DateTime" />.
/// </summary>
#endif
public readonly struct DateTimeOffsetBuilder(DateTimeOffset value)
{
	private readonly DateTimeOffset _value = value;

	/// <summary>
	///     Implicitly casts the <see cref="DateTimeBuilder" /> to a <see cref="DateTime" />.
	/// </summary>
	public static implicit operator DateTime(DateTimeOffsetBuilder builder)
		=> builder._value.DateTime;

	/// <summary>
	///     Implicitly casts the <see cref="DateTimeBuilder" /> to a <see cref="DateTimeOffset" />.
	/// </summary>
	public static implicit operator DateTimeOffset(DateTimeOffsetBuilder builder)
		=> builder._value;

#if NET8_0_OR_GREATER
	/// <summary>
	///     Implicitly casts the <see cref="DateTimeBuilder" /> to a <see cref="DateOnly" />.
	/// </summary>
	public static implicit operator DateOnly(DateTimeOffsetBuilder builder)
		=> DateOnly.FromDateTime(builder._value.DateTime);
#endif

#if NET8_0_OR_GREATER
	/// <summary>
	///     Implicitly casts the <see cref="DateTimeBuilder" /> to a <see cref="TimeOnly" />.
	/// </summary>
	public static implicit operator TimeOnly(DateTimeOffsetBuilder builder)
		=> TimeOnly.FromDateTime(builder._value.DateTime);
#endif

	/// <summary>
	///     Adds a <see cref="TimeSpan" /> to the <see cref="DateTime" />.
	/// </summary>
	public static DateTimeOffsetBuilder operator +(DateTimeOffsetBuilder builder, TimeSpan time)
		=> new(builder._value + time);

	/// <summary>
	///     Subtracts a <see cref="TimeSpan" /> from the <see cref="DateTime" />.
	/// </summary>
	public static DateTimeOffsetBuilder operator -(DateTimeOffsetBuilder builder, TimeSpan time)
		=> new(builder._value - time);
}