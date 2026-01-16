package com.openpromt.coffeee.swf2023.openpromtserver.copyright.dto;

import com.openpromt.coffeee.swf2023.openpromtserver.ownticket.dto.OwnTicketResponseDto;
import com.openpromt.coffeee.swf2023.openpromtserver.user.dto.UserResponseDto;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class CopyRightResponseDto {

    private String copyrightId;
    private UserResponseDto user;
    private OwnTicketResponseDto ownTicket;
    private String copyrightTitle;
    private String privKey;
    private String pubKey;
}