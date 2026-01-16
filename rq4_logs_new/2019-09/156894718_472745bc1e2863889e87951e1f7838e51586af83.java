package com.bin448.backend.converter;

import com.bin448.backend.entity.DTOentity.InvitationDTO;
import com.bin448.backend.entity.Invitation;
import com.bin448.backend.entity.PlaneSeat;
import com.bin448.backend.entity.User;

public abstract class InvitationConverter extends AbstractConverter{

    public static InvitationDTO fromEntity(Invitation e) {
        InvitationDTO invitationDTO = new InvitationDTO();
        invitationDTO.setId(e.getId());
        invitationDTO.setSeatId(e.getSeat().getSeatId());
        invitationDTO.setUserInivte(e.getInviting().getUsername());
        invitationDTO.setUserReceive(e.getReceiving().getUsername());
        invitationDTO.setStatus(e.isInvitationStatus());
        return invitationDTO;
    }

    public static Invitation toEntity(InvitationDTO d) {
        Invitation invitation = new Invitation();
        invitation.setId(d.getId());
        PlaneSeat seat = new PlaneSeat();
        seat.setSeatId(d.getSeatId());
        invitation.setSeat(seat);
        User inviting = new User();
        inviting.setUsername(d.getUserInivte());
        invitation.setInviting(inviting);
        User receiving = new User();
        receiving.setUsername(d.getUserReceive());
        invitation.setReceiving(receiving);
        invitation.setInvitationStatus(d.isStatus());
        return invitation;
    }
}