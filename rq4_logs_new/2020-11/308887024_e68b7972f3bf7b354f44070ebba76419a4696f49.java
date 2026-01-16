package com.marketplace.domain.userprofile.controller;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.marketplace.domain.userprofile.DisplayName;
import com.marketplace.domain.userprofile.FullName;
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

    @JsonIgnore
    public DisplayName displayName() {
        return new DisplayName(displayName);
    }

    @JsonIgnore
    public FullName fullName() {
        return new FullName(firstName, middleName, lastName);
    }
}