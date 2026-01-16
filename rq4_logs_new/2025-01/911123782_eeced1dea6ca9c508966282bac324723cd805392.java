package com.roomfit.be.user;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public class UserDTO {
    /**
     * when userDTO deserialize to User Entity, the password in User DTO should be encrypted.
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Create{
        private String nickname;
        private String email;
        private String password;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Response{
        private Long id;
        private String nickname;
        private String email;
        private String password;
        public static Response of(User user) {
            return Response.builder()
                    .id(user.getId())
                    .nickname(user.getNickname())
                    .email(user.getEmail())
                    .password(user.getPassword())
                    .build();
        }
    }
}