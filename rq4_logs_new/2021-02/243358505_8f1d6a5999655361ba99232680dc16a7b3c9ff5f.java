package mybase.domain.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import mybase.domain.types.UserRole;

import java.time.LocalDateTime;
import java.util.Set;

@Data
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class UserAccountDto extends BaseDto{

    private String userID;

    private String username;

    private String email;

    private Boolean isEnabled;

    private LocalDateTime registrationDate;

    private Set<UserRole> roles;

}