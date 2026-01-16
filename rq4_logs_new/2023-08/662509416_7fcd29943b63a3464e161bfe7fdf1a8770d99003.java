package ru.practicum.shareit.booking.repository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Repository;
import ru.practicum.shareit.booking.dto.BookingShortDto;

import java.time.LocalDateTime;
import java.util.Map;

@Repository
public class BookingRepositoryForCustomMethod {
    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public BookingRepositoryForCustomMethod(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public BookingShortDto getNextBooking(int itemId) {
        String sqlQuery = "select b.id, b.booker_id from bookings as b where b.item_id=? and (b.start_date > ?)" +
                " order by abs(timestampdiff(second, b.start_date, ?)) limit 1";
        return getBookingShortDto(itemId, sqlQuery);
    }

    public BookingShortDto getLastBooking(int itemId) {
        String sqlQuery = "select b.id, b.booker_id from bookings as b where b.item_id=? and (b.end_date < ?)" +
                " order by abs(timestampdiff(second, b.end_date, ?)) desc limit 1";
        return getBookingShortDto(itemId, sqlQuery);
    }

    private BookingShortDto getBookingShortDto(int itemId, String sqlQuery) {
        Map<String, Object> mapOfColumns = null;
        try {
            mapOfColumns = jdbcTemplate.queryForMap(sqlQuery, itemId, LocalDateTime.now(), LocalDateTime.now());
        } catch (DataAccessException e) {
            return null;
        }
        return new BookingShortDto((int) mapOfColumns.get("id"), (int) mapOfColumns.get("booker_id"));
    }
}