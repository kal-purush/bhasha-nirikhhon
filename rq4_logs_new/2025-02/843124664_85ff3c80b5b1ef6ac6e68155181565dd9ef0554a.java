package local.pms.authservice.dto.authuser;

import jakarta.validation.constraints.Size;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;

import java.util.UUID;

public record UserDetailsDto(
        @NotBlank(message = "First name is required")
        @Size(min = 3, max = 255, message = "First name must be between 3 and 255 characters")
        String firstName,

        @NotBlank(message = "Last name is required")
        @Size(min = 3, max = 255, message = "Last name must be between 3 and 255 characters")
        String lastName,

        @Email(message = "Email must be a valid email address")
        @NotBlank(message = "Email is required")
        @Size(min = 3, max = 255, message = "Email must be between 3 and 255 characters")
        String email,

        @NotBlank(message = "Authentication user identifier is required")
        @Size(min = 36, max = 36, message = "Authentication user identifier must be 36 characters")
        UUID authUserId) {}