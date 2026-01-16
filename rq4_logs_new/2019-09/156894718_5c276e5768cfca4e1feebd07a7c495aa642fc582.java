package com.bin448.backend.converter;

import com.bin448.backend.entity.DTOentity.FrendshipDTO;
import com.bin448.backend.entity.Friendship;
import com.bin448.backend.entity.User;
import com.bin448.backend.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;

public class FriendshipConverter extends AbstractConverter {

    @Autowired
    private static UserRepository a;

    @Autowired
    public void setUserRepo(UserRepository userRepo) {
        FriendshipConverter.a = userRepo;
    }

    public static FrendshipDTO fromEntity(Friendship e) {
        FrendshipDTO newDTOF = new FrendshipDTO();
        newDTOF.setSenderUsername(e.getSender().getUsername());
        newDTOF.setReceiverUsername(e.getReceiver().getUsername());
        newDTOF.setAreFriends(e.isAreFriends());
        return newDTOF;
    }
    public static Friendship toEntity(FrendshipDTO d) {
        Friendship newF = new Friendship();
        User newUser = a.findByUsername(d.getSenderUsername());
        newF.setSender(newUser);
        User newUser2 = a.findByUsername(d.getReceiverUsername());
        newF.setReceiver(newUser2);
        newF.setAreFriends(d.isAreFriends());
        return newF;
    }
}