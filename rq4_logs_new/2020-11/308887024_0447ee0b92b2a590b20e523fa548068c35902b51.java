package com.marketplace.controller.userprofile;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class CreateUserProfileCommand {
    private String firstName;
    private String lastName;
    private String middleName;
    private String displayName;
    private String photoUrl;

    public static CreateUserProfileCommand from(CreateUserProfileDto createUserProfileDto) {
        return CreateUserProfileCommand.builder()
                .displayName(createUserProfileDto.getDisplayName())
                .firstName(createUserProfileDto.getFirstName())
                .lastName(createUserProfileDto.getLastName())
                .middleName(createUserProfileDto.getMiddleName())
                .displayName(createUserProfileDto.getDisplayName())
                .photoUrl(createUserProfileDto.getPhotoUrl())
                .build();
    }
}