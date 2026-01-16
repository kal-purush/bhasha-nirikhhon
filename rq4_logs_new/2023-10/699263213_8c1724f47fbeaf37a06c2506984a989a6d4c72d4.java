package ru.practicum.ewm.rating.dao;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class VoteId implements Serializable {

    private Integer eventId;

    private Long userId;
}