package ru.practicum.main.dto.event;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;
import org.springframework.format.annotation.DateTimeFormat;

import javax.validation.constraints.Positive;
import javax.validation.constraints.PositiveOrZero;
import java.time.LocalDateTime;
import java.util.Objects;

@Value
@Builder
@Jacksonized
public class PublicEventSearchFilter {
    String text;
    Long[] users;
    Long[] categories;
    @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    LocalDateTime rangeStart;
    @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    LocalDateTime rangeEnd;
    Boolean paid;
    String sort;
    Boolean onlyAvailable;
    @PositiveOrZero(message = "From value must be a positive or zero")
    Integer from;
    @Positive(message = "Size value must be a positive")
    Integer size;

    @JsonCreator
    public PublicEventSearchFilter(@JsonProperty(value = "text") String text,
                                   @JsonProperty(value = "users") Long[] users,
                                   @JsonProperty(value = "categories") Long[] categories,
                                   @JsonProperty(value = "rangeStart")
                                   @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime rangeStart,
                                   @JsonProperty(value = "rangeEnd")
                                   @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime rangeEnd,
                                   @JsonProperty(value = "paid") Boolean paid,
                                   @JsonProperty(value = "sort") String sort,
                                   @JsonProperty(value = "onlyAvailable") Boolean onlyAvailable,
                                   @JsonProperty(value = "from") Integer from,
                                   @JsonProperty(value = "size") Integer size) {
        this.text = text;
        this.users = users;
        this.categories = categories;
        this.rangeStart = rangeStart;
        this.rangeEnd = rangeEnd;
        this.paid = paid;
        this.sort = sort;
        this.onlyAvailable = Objects.requireNonNullElse(onlyAvailable, false);
        this.from = Objects.requireNonNullElse(from, 0);
        this.size = Objects.requireNonNullElse(size, 10);
    }
}