/*
 * DT-Tool
 * Copyright (c) 2024-present Carsten Rambow
 * mailto:developer AT elomagic DOT de
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.elomagic.dttool;

import jakarta.annotation.Nonnull;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public final class TimeUtil {

    private TimeUtil() {}

    @Nonnull
    public static LocalDate parseMonthPattern(@Nonnull String monthPattern) {
        int year = Integer.parseInt(monthPattern.substring(0, 4));
        int month = Integer.parseInt(monthPattern.substring(5, 7));

        return LocalDate.of(year, month, 1);
    }

    @Nonnull
    public static String toMonthPattern(@Nonnull LocalDate localDate) {
        return localDate.format(DateTimeFormatter.ofPattern("yyyy-MM"));
    }

}