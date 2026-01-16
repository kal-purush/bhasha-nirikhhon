package ru.practicum.shareit.booking;

import java.util.List;
import java.util.stream.Collectors;

import ru.practicum.shareit.booking.dto.BookingDtoForItem;
import ru.practicum.shareit.booking.dto.BookingDtoRequest;
import ru.practicum.shareit.booking.dto.BookingDtoResponse;
import ru.practicum.shareit.item.model.Item;
import ru.practicum.shareit.user.model.User;


public class BookingMapper {
    public static Booking toBooking(BookingDtoRequest bookingDtoRequest, Item inputItem, User inputUser) {
        return Booking.builder()
                .id(bookingDtoRequest.getId())
                .start(bookingDtoRequest.getStart())
                .end(bookingDtoRequest.getEnd())
                .item(inputItem)
                .booker(inputUser)
                .build();
    }

    public static BookingDtoResponse toBookingDtoResponse(Booking booking) {
        return BookingDtoResponse.builder()
                .id(booking.getId())
                .start(booking.getStart())
                .end(booking.getEnd())
                .status(booking.getStatus())
                .item(toItemDto(booking.getItem()))
                .booker(toBookerDto(booking.getBooker()))
                .build();
    }

    public static BookingDtoForItem toBookingDtoForItem(Booking booking) {
        return BookingDtoForItem.builder()
                .id(booking.getId())
                .start(booking.getStart())
                .end(booking.getEnd())
                .status(booking.getStatus())
                .itemId(booking.getItem().getId())
                .bookerId(booking.getBooker().getId())
                .build();
    }

    public static List<BookingDtoResponse> mapToBookingDtoResponse(List<Booking> bookings) {
        return bookings.stream()
                .map(BookingMapper::toBookingDtoResponse)
                .collect(Collectors.toList());
    }

    private static BookingDtoResponse.Item toItemDto(Item item) {
        return new BookingDtoResponse.Item(item.getId(), item.getName());
    }

    private static BookingDtoResponse.Booker toBookerDto(User user) {
        return new BookingDtoResponse.Booker(user.getId(), user.getName());
    }
}