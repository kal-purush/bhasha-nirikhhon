package com.github.piasy.base.model.jsr310;

import android.content.ContentValues;
import android.database.Cursor;
import android.support.annotation.NonNull;
import com.squareup.sqldelight.ColumnAdapter;
import org.threeten.bp.ZonedDateTime;
import org.threeten.bp.format.DateTimeFormatter;

public class ZonedDateTimeDelightAdapter implements ColumnAdapter<ZonedDateTime> {

    private final DateTimeFormatter mDateTimeFormatter;

    public ZonedDateTimeDelightAdapter(DateTimeFormatter dateTimeFormatter) {
        mDateTimeFormatter = dateTimeFormatter;
    }

    @NonNull
    @Override
    public ZonedDateTime map(Cursor cursor, int columnIndex) {
        return mDateTimeFormatter.parse(cursor.getString(columnIndex), ZonedDateTime.FROM);
    }

    @Override
    public void marshal(ContentValues values, String key, @NonNull ZonedDateTime value) {
        values.put(key, mDateTimeFormatter.format(value));
    }
}