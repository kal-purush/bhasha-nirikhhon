package com.challenger.fridge.dto.member;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class MemberInfoRequest {

    private String password;
    private Long mainStorageId;
}