package ru.practicum.shareit.booking.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.bind.MethodArgumentNotValidException;
import ru.practicum.shareit.booking.BookingController;
import ru.practicum.shareit.booking.dto.BookingDto;
import ru.practicum.shareit.booking.dto.NewBookingDto;
import ru.practicum.shareit.booking.model.BookingState;
import ru.practicum.shareit.booking.service.BookingService;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.*;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static ru.practicum.shareit.common.model.Constants.USER_HEADER;

@WebMvcTest(controllers = BookingController.class)
class BookingControllerTest {

    @MockBean
    private BookingService bookingService;

    @Autowired
    private MockMvc mvc;

    @Autowired
    private ObjectMapper objectMapper;

    private long userId;

    private NewBookingDto newBookingDto;

    private BookingDto bookingDto;


    @BeforeEach
    void setUp() {
        userId = 1;
        newBookingDto = NewBookingDto.builder()
                .itemId(1L)
                .start(LocalDateTime.now().plusDays(1))
                .end(LocalDateTime.now().plusDays(3))
                .build();
        bookingDto = new BookingDto();
    }


    @Test
    @SneakyThrows
    void addNewBooking() {
        when(bookingService.create(userId, newBookingDto))
                .thenReturn(bookingDto);

        mvc.perform(post("/bookings")
                        .header(USER_HEADER, userId)
                        .contentType(APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(newBookingDto)))
                .andExpect(status().isOk())
                .andExpect(content().contentType(APPLICATION_JSON))
                .andExpect(content().string(objectMapper.writeValueAsString(bookingDto)));

        verify(bookingService, times(1)).create(userId, newBookingDto);
    }

    @Test
    @SneakyThrows
    void checkStartDayOfBooking() {
        newBookingDto.setStart(LocalDateTime.now().minusDays(2));
        mvc.perform(post("/bookings")
                        .header(USER_HEADER, userId)
                        .contentType(APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(newBookingDto)))
                .andExpect(status().isBadRequest())
                .andExpect(result -> assertInstanceOf(MethodArgumentNotValidException.class, result.getResolvedException()));

        verify(bookingService, never()).create(any(), any());
    }

    @Test
    @SneakyThrows
    void updateBooking() {
        Long bookingId = 2L;
        Boolean approved = true;
        when(bookingService.patch(userId, bookingId, approved))
                .thenReturn(bookingDto);

        mvc.perform(patch("/bookings/{bookingId}", bookingId)
                        .header(USER_HEADER, userId)
                        .param("approved", approved.toString()))
                .andExpect(status().isOk())
                .andExpect(content().contentType(APPLICATION_JSON))
                .andExpect(content().string(objectMapper.writeValueAsString(bookingDto)));

        verify(bookingService, times(1)).patch(userId, bookingId, approved);
    }

    @Test
    @SneakyThrows
    void getBookingById() {
        Long bookingId = 2L;
        when(bookingService.get(userId, bookingId))
                .thenReturn(bookingDto);

        mvc.perform(get("/bookings/{bookingId}", bookingId)
                        .header(USER_HEADER, userId))
                .andExpect(status().isOk())
                .andExpect(content().contentType(APPLICATION_JSON))
                .andExpect(content().string(objectMapper.writeValueAsString(bookingDto)));

        verify(bookingService, times(1)).get(userId, bookingId);
    }

    @Test
    @SneakyThrows
    void findAlBookings() {
        BookingState state = BookingState.FUTURE;
        int from = 0;
        int size = 50;
        when(bookingService.findAll(userId, state, from, size))
                .thenReturn(List.of(bookingDto));

        mvc.perform(get("/bookings")
                        .header(USER_HEADER, userId)
                        .param("state", state.name())
                        .param("from", String.valueOf(from))
                        .param("size", String.valueOf(size)))
                .andExpect(status().isOk())
                .andExpect(content().contentType(APPLICATION_JSON))
                .andExpect(content().string(objectMapper.writeValueAsString(List.of(bookingDto))));

        verify(bookingService, times(1)).findAll(userId, state, from, size);
    }

    @Test
    @SneakyThrows
    void findAllOwnerBookings() {
        BookingState state = BookingState.FUTURE;
        int from = 1;
        int size = 5;
        when(bookingService.findAllOwnerBookings(userId, state, from, size))
                .thenReturn(List.of(bookingDto));

        mvc.perform(get("/bookings/owner")
                        .header(USER_HEADER, userId)
                        .param("state", state.name())
                        .param("from", String.valueOf(from))
                        .param("size", String.valueOf(size)))
                .andExpect(status().isOk())
                .andExpect(content().contentType(APPLICATION_JSON))
                .andExpect(content().string(objectMapper.writeValueAsString(List.of(bookingDto))));

        verify(bookingService, times(1)).findAllOwnerBookings(userId, state, from, size);
    }
}