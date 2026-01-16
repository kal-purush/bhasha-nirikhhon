package ru.practicum.gateway.clientTest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import ru.practicum.gateway.booking.client.BookingClient;
import ru.practicum.gateway.booking.dto.BookingDto;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BookingClientTest {

    private static final String path = "http://localhost:8081/bookings";

    @Mock
    private BookingClient bookingClient;
    private static BookingDto bookingDto;
    private final long userId = 1L;

    @BeforeEach
    void setUp() {
        bookingDto = new BookingDto();

        bookingClient = spy(new BookingClient(path, new RestTemplateBuilder()));
    }

    @Test
    void addBookingClientTest() {
        ResponseEntity<Object> expectedResponse = ResponseEntity.ok().build();

        when(bookingClient.addBooking(anyLong(), any())).thenReturn(expectedResponse);

        assertThat(bookingClient.addBooking(userId, bookingDto).getStatusCode(), equalTo(HttpStatus.OK));
    }

    @Test
    void changeStatusClientTest() {
        ResponseEntity<Object> expectedResponse = ResponseEntity.ok().build();

        Boolean approved = true;
        long bookingId = 123L;

        Map<String, Object> parameters = Map.of("approved", approved);

        when(bookingClient.patch("/" + bookingId + "?approved={approved}", userId, parameters, null))
                .thenReturn(expectedResponse);

        ResponseEntity<Object> actualResponse = bookingClient.changeBookingStatus(userId, approved, bookingId);

        assertThat(actualResponse, equalTo(expectedResponse));
    }

    @Test
    void getBookingClientTest() {
        ResponseEntity<Object> expectedResponse = ResponseEntity.ok().build();

        when(bookingClient.getBooking(anyLong(), anyLong())).thenReturn(expectedResponse);

        assertThat(bookingClient.getBooking(2L, userId).getStatusCode(), equalTo(HttpStatus.OK));
    }
}
