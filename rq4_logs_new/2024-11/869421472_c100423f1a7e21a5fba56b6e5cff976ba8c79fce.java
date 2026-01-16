package com.cinema.dto.response;

import com.cinema.entity.Room;
import com.cinema.entity.User;
import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.persistence.Column;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.OneToMany;
import jakarta.persistence.OneToOne;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class CinemaResponse {
    private Integer id;

    private String name;

    private String address;

    @JsonIgnore
    private User admin;

    @JsonIgnore
    private List<Room> rooms;

    private LocalDateTime createdAt;

    private LocalDateTime lastModifyAt;

    private String createdBy;

    private String lastModifyBy;
}