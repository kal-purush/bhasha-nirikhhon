package com.walkbuddies.backend.club.dto.form;

import lombok.*;

import java.time.LocalDate;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ClubUpdateParameter {
    private long clubId;
    private String clubName;
    private long townId;
    private long ownerId;
    private int members;
    private int membersLimit;
    private int accessLimit;
    private int needGrant;
    private boolean isSuspended;
    private LocalDate regDate;
    private LocalDate modDate;

    private long memberId;
}