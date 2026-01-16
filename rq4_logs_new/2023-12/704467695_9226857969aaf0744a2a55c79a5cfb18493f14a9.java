package ru.yandex.practicum.filmorate.dao.impl;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.filmorate.dao.EventsStorage;
import ru.yandex.practicum.filmorate.exception.DataNotFoundException;
import ru.yandex.practicum.filmorate.model.Event;
import ru.yandex.practicum.filmorate.model.EventOperation;
import ru.yandex.practicum.filmorate.model.EventType;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

@Component
public class EventsDaoImpl implements EventsStorage {

    private final JdbcTemplate jdbcTemplate;

    public EventsDaoImpl(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public List<Event> findUserIdEvents(Long id) {
        String sql = "SELECT * FROM Events WHERE user_id = ? ORDER BY time_stamp";
        return jdbcTemplate.query(sql, this::makeEvent, id);
    }

    @Override
    public Event createUserIdEvents(Long entityId, Long userId, EventType eventType, EventOperation operation) {
        String sqlQuery = "INSERT INTO Events (entity_id, user_id, event_type, operation, time_stamp) VALUES (?, ?, ?, ?, ?)";
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(connection -> {
            PreparedStatement stmt = connection.prepareStatement(sqlQuery, new String[]{"event_id"});
            stmt.setLong(1, entityId);
            stmt.setLong(2, userId);
            stmt.setString(3, eventType.toString());
            stmt.setString(4, operation.toString());
            stmt.setTimestamp(5, Timestamp.from(Instant.now()));
            return stmt;
        }, keyHolder);
        if (keyHolder.getKey() != null) {
            return getUserIdEvent(Objects.requireNonNull(keyHolder.getKey()).longValue());
        } else {
            throw new DataNotFoundException("The Event has not been add for userId "
                    + userId + " - " + entityId);
        }
    }

    @Override
    public Event getUserIdEvent(Long entityId) {
        SqlRowSet genreRows = jdbcTemplate.queryForRowSet("SELECT * FROM Events WHERE event_id = ?", entityId);
        if (genreRows.next()) {
            long id = genreRows.getLong("event_id");
            Long entity = genreRows.getLong("entity_id");
            Long userId = genreRows.getLong("user_id");
            String eventType = genreRows.getString("event_type");
            String operation = genreRows.getString("operation");
            Long timeStamp = Objects.requireNonNull(genreRows.getTimestamp("time_stamp")).toInstant().toEpochMilli();
            return Event.builder().eventId(id).entityId(entity).userId(userId).eventType(EventType.valueOf(eventType))
                    .operation(EventOperation.valueOf(operation)).timestamp(timeStamp).build();
        } else {
            throw new DataNotFoundException("The Event has not been find with id " + entityId);
        }
    }

    private Event makeEvent(ResultSet rs, int rowNum) throws SQLException {
        long id = rs.getLong("event_id");
        Long entityId = rs.getLong("entity_id");
        Long userId = rs.getLong("user_id");
        String eventType = rs.getString("event_type");
        String operation = rs.getString("operation");
        Long timeStamp = rs.getTimestamp("time_stamp").toInstant().toEpochMilli();
        return Event.builder().eventId(id).entityId(entityId).userId(userId).eventType(EventType.valueOf(eventType))
                .operation(EventOperation.valueOf(operation)).timestamp(timeStamp).build();
    }
}