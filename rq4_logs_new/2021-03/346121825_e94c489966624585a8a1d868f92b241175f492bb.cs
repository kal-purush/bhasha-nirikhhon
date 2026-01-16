/*
* Copyright © 2021 MyNihongo
*/

using System;
using System.Collections.Generic;

namespace MyNihongo.Option
{
	public readonly struct Optional<T> : IEquatable<Optional<T>>
	{
		private static readonly Optional<T> Empty = default;

		private readonly T _value;

		private Optional(T value, bool hasValue)
		{
			_value = value;
			HasValue = hasValue;
		}

		public bool HasValue { get; }

		public T Value => HasValue ? _value : throw new InvalidOperationException("Optional<T> has no value");

		public static Optional<T> Of(T value) =>
			new(value, true);

		public static Optional<T> None() =>
			Empty;

		public static implicit operator Optional<T>(T value) =>
			new(value, true);

		public static explicit operator T(Optional<T> value) =>
			value.Value;

		public static bool operator ==(Optional<T> left, Optional<T> right) =>
			left.Equals(right);

		public static bool operator !=(Optional<T> left, Optional<T> right) =>
			!left.Equals(right);

		/// <inheritdoc />
		public bool Equals(Optional<T> other)
		{
			if (!HasValue)
				return !other.HasValue;

			if (!other.HasValue)
				return false;

			return HasValue.Equals(other.HasValue) && EqualityComparer<T>.Default.Equals(_value, other._value);
		}

		/// <inheritdoc />
		public override bool Equals(object? obj)
		{
			if (obj == null)
				return false;

			return obj is Optional<T> optional &&
				   Equals(optional);
		}

		/// <inheritdoc />
		public override int GetHashCode()
		{
			unchecked
			{
				return _value == null ? 0 : (HasValue.GetHashCode() * 777) ^ EqualityComparer<T>.Default.GetHashCode(_value);
			}
		}

		/// <inheritdoc />
		public override string ToString() =>
			HasValue ? _value?.ToString() ?? string.Empty : "<None>";
	}
}