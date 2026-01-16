package com.bahubba.bahubbabookclub.model.entity;

import com.bahubba.bahubbabookclub.model.enums.NotificationType;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Set;
import java.util.UUID;

@Entity
@Table(name = "notification")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Notification implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    @Id
    @Column(nullable = false, unique = true)
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @ManyToOne(optional = false)
    @JoinColumn(name = "source_reader_id")
    private Reader sourceReader;

    @ManyToOne(optional = false)
    @JoinColumn(name = "target_reader_id")
    private Reader targetReader;

    @ManyToOne
    @JoinColumn(name = "book_club_id")
    private BookClub bookClub;

    @Column(nullable = false)
    private NotificationType type;

    @Column(name = "action_link")
    private String actionLink;

    @Column
    private LocalDateTime generated;

    @OneToMany(mappedBy = "notification", fetch = FetchType.LAZY)
    private Set<NotificationViews> views;
}