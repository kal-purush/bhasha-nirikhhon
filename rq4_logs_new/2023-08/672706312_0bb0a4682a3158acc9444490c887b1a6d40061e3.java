package com.openpromt.coffeee.swf2023.openpromtserver.ownticket.dto;

import com.openpromt.coffeee.swf2023.openpromtserver.copyright.entity.Copyright;
import com.openpromt.coffeee.swf2023.openpromtserver.ownticket.entity.OwnTicket;
import com.openpromt.coffeee.swf2023.openpromtserver.user.entity.User;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class OwnTicketResponseDto {
    private Long own_id;
    private User userId;
    private Copyright copyrightId;

    public static OwnTicketResponseDto convertToDto(OwnTicket ownTicket){
        return OwnTicketResponseDto.builder()
                .own_id(ownTicket.getOwn_id())
                .userId(ownTicket.getUserId())
                .copyrightId(ownTicket.getCopyrightId())
                .build();
    }

    public static List<OwnTicketResponseDto> convertToDtoList(List<OwnTicket> tickets){
        Stream<OwnTicket> stream = tickets.stream();
        return stream.map((ticket) -> convertToDto(ticket)).collect(Collectors.toList());
    }

}