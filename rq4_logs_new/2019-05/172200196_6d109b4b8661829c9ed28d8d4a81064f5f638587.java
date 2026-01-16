/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ws2ten1.chunks;

import java.util.Locale;
import java.util.Optional;

/**
 * Enumeration for sort directions.
 */
public enum Direction {
	
	ASC, DESC;
	
	/**
	 * Returns whether the direction is ascending.
	 *
	 * @return {@code true} if this is ascending direction
	 */
	public boolean isAscending() {
		return this.equals(ASC);
	}
	
	/**
	 * Returns whether the direction is descending.
	 *
	 * @return {@code true} if this is descending direction
	 */
	public boolean isDescending() {
		return this.equals(DESC);
	}
	
	/**
	 * Returns the {@link Direction} enum for the given {@link String} value.
	 *
	 * @param value {@code "ASC"} or {@code "DESC"}
	 * @return parsed {@link Direction}
	 * @throws IllegalArgumentException in case the given value cannot be parsed into an enum value.
	 */
	public static Direction fromString(String value) {
		
		try {
			return Direction.valueOf(value.toUpperCase(Locale.US));
		} catch (Exception e) { // NOPMD
			throw new IllegalArgumentException(String.format(Locale.US,
					"Invalid value '%s' for orders given! Has to be either 'desc' or 'asc' (case insensitive).", value),
					e);
		}
	}
	
	/**
	 * Returns the {@link Direction} enum for the given {@link String} or null if it cannot be parsed into an enum
	 * value.
	 *
	 * @param value {@code "ASC"} or {@code "DESC"}
	 * @return parsed {@link Direction} or {@link Optional#empty()}
	 */
	public static Optional<Direction> fromOptionalString(String value) {
		
		try {
			return Optional.of(fromString(value));
		} catch (IllegalArgumentException e) {
			return Optional.empty();
		}
	}
}