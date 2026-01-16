package ru.practicum.shareit.booking.util;

import ru.practicum.shareit.booking.model.BookingStatus;

import javax.persistence.AttributeConverter;
import javax.persistence.Converter;
import java.util.stream.Stream;

@Converter(autoApply = true)
public class BookingStatusConverter implements AttributeConverter<BookingStatus,String> {
    @Override
    public String convertToDatabaseColumn(BookingStatus bookingStatus) {
        if (bookingStatus == null) {
            return null;
        }
        return bookingStatus.getMessage();
    }

    @Override
    public BookingStatus convertToEntityAttribute(String s) {
        if (s == null) return null;

        return Stream.of(BookingStatus.values())
                .filter(status -> status.getMessage().equals(s))
                .findFirst()
                .orElseThrow(IllegalArgumentException::new);
    }
}
