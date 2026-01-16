package com.brainstrom.meokjang.dto;

import com.brainstrom.meokjang.domain.User;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class SignupRequestDTO {
    private String userName;
    private String phoneNumber;
    private String snsType;
    private String snsKey;
    private String location;
    private Double latitude;
    private Double longitude;
    private Integer gender;

    public User toUser() {
        User user = new User();
        user.setUserName(userName);;
        user.setPhoneNumber(phoneNumber);
        user.setSnsType(snsType);
        user.setSnsKey(snsKey);
        user.setLocation(location);
        user.setLatitude(latitude);
        user.setLongitude(longitude);
        user.setGender(gender);
        user.setReliability((float)50);
        return user;
    }
}