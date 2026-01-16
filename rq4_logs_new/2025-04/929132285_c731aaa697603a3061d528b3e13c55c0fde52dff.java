package nl.optifit.backendservice.model;

import com.fasterxml.jackson.annotation.*;
import jakarta.persistence.*;
import jakarta.validation.constraints.*;
import jakarta.validation.constraints.NotBlank;
import lombok.*;
import org.hibernate.validator.constraints.*;

import java.util.UUID;

@Entity
@Table(name = "videos", indexes = @Index(columnList = "id"))
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SessionVideo {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;
    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private ExerciseType exerciseType;
    @Min(1) @Max(3)
    private Integer score;
    @URL(message = "Invalid URL format")
    @NotBlank(message = "Video URL must not be blank")
    private String videoUrl;
}