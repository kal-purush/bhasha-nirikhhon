package fiftyfive.administration.usermanagement.entity;

import jakarta.persistence.*;
import lombok.Data;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;


@Entity
@Table(name = "deleted_user", schema = "public")
@Data
public class DeletedUser {

    @Column(name = "username", nullable = false)
    private String username;

    @Column(name = "first_name", nullable = false)
    private String firstName;

    @Column(name = "last_name", nullable = false)
    private String lastName;

    @Column(name = "role", nullable = false)
    private String role;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    @Column(name = "password", nullable = false)

    private String password;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long deletedUserId;

    @Column(name = "id", nullable = false)
    private Long userId;

    @Column(name = "deleted_at", nullable = false)
    @CreationTimestamp
    private LocalDateTime deletedAt;
}