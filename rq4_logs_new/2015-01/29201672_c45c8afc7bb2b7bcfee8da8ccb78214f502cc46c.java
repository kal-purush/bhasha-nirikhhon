/*
 * Copyright [2015] [DoubleDutch]
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

package me.doubledutch.options.accessor;

import org.junit.Test;

import java.util.Calendar;
import java.util.Date;

import static java.util.Calendar.*;
import static org.junit.Assert.assertEquals;

public class TimestampFieldAccessorTest {
    final static Long januaryFirst2014 = 1388534400000L;

    TimestampFieldAccessor accessor = new TimestampFieldAccessor();

    @Test
    public void testSet_date() throws Exception {
        ObjectWithDate objectWithDate = new ObjectWithDate();
        accessor.set(objectWithDate, "timestamp", String.valueOf(januaryFirst2014));

        Date expectedDate = new Date(januaryFirst2014);
        assertEquals(expectedDate, objectWithDate.timestamp);
    }

    @Test
    public void testSet_calendar() throws Exception {
        ObjectWithCalendar objectWithCalendar = new ObjectWithCalendar();
        accessor.set(objectWithCalendar, "timestamp", String.valueOf(januaryFirst2014));

        Calendar expectedCalendar = TimestampFieldAccessor.createCalendarWithMillis(januaryFirst2014);
        assertEquals(expectedCalendar, objectWithCalendar.timestamp);
    }

    @Test
    public void testSet_long() throws Exception {
        ObjectWithLong objectWithLong = new ObjectWithLong();
        accessor.set(objectWithLong, "timestamp", String.valueOf(januaryFirst2014));
        assertEquals(januaryFirst2014, objectWithLong.timestamp);
    }

    @Test
    public void testSet_withLexicalDateFormat() throws Exception {
        ObjectWithLong objectWithLong = new ObjectWithLong();
        accessor.set(objectWithLong, "timestamp", "2014-01-01T00:00:00Z");
        assertEquals(januaryFirst2014, objectWithLong.timestamp);
    }

    @Test
    public void testCreateCalendarWithMillis() {
        Calendar calendar = TimestampFieldAccessor.createCalendarWithMillis(januaryFirst2014);
        assertEquals(2014, calendar.get(YEAR));
        assertEquals(0, calendar.get(MONTH));
        assertEquals(1, calendar.get(DAY_OF_MONTH));
    }

    class ObjectWithCalendar {
        Calendar timestamp;
    }

    class ObjectWithDate {
        Date timestamp;
    }

    class ObjectWithLong {
        Long timestamp;
    }
}