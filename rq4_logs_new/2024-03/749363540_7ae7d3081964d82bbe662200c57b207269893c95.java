package ru.practicum.shareit.booking;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.practicum.shareit.booking.dto.BookingDto;
import ru.practicum.shareit.booking.dto.BookingFromUserDto;
import ru.practicum.shareit.booking.dto.BookingInfoDto;
import ru.practicum.shareit.exception.NotFoundException;
import ru.practicum.shareit.exception.ValidationException;
import ru.practicum.shareit.item.ItemMapper;
import ru.practicum.shareit.item.ItemRepository;
import ru.practicum.shareit.item.model.Item;
import ru.practicum.shareit.user.User;
import ru.practicum.shareit.user.UserMapper;
import ru.practicum.shareit.user.UserRepository;

import java.util.ArrayList;
import java.util.List;

@Component
public class BookingMapper {
    private static ItemRepository itemRepository;
    private static UserRepository userRepository;

    @Autowired
    public BookingMapper(ItemRepository itemRepository, UserRepository userRepository) {
        this.itemRepository = itemRepository;
        this.userRepository = userRepository;
    }

    public static Booking toBooking(Long bookerId, BookingFromUserDto bookingDto) {

        Booking booking = new Booking();
        booking.setStart(bookingDto.getStart());
        booking.setEnd(bookingDto.getEnd());
        Item item = itemRepository.findById(bookingDto.getItemId()).orElseThrow(() ->
                new NotFoundException("Запрашиваемый обьект отсутствует в базе"));
        if (!item.isAvailable()) {
            throw new ValidationException("Запрашиваемый обьект забронирован");
        }
        booking.setItem(item);
        User booker = userRepository.findById(bookerId)
                .orElseThrow(() -> new NotFoundException("Пользователь не найден"));
        booking.setBooker(booker);

        booking.setStatus(BookingState.WAITING);
        booking.setApproved(false);

        return booking;

    }

    public static BookingDto toBookingDto(Booking booking) {

        BookingDto bookingDto = new BookingDto();
        bookingDto.setId(booking.getId());
        bookingDto.setStart(booking.getStart());
        bookingDto.setEnd(booking.getEnd());
        bookingDto.setItem(ItemMapper.toItemDto(booking.getItem()));
        bookingDto.setBooker(UserMapper.toUserDto(booking.getBooker()));
        bookingDto.setStatus(booking.getStatus());
        bookingDto.setApproved(booking.getApproved());

        return bookingDto;
    }

    public static List<BookingDto> toBookingDto(Iterable<Booking> bookings) {
        List<BookingDto> result = new ArrayList<>();

        for (Booking booking : bookings) {
            result.add(toBookingDto(booking));
        }

        return result;
    }

    public static BookingInfoDto toBookingInfoDto(Booking booking) {

        BookingInfoDto bookinginfoDto = new BookingInfoDto();
        bookinginfoDto.setId(booking.getId());
        bookinginfoDto.setStart(booking.getStart());
        bookinginfoDto.setEnd(booking.getEnd());
        bookinginfoDto.setBookerId(booking.getBooker().getId());

        return bookinginfoDto;
    }
}
