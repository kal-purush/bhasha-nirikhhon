package com.hoaxify.ws.hoax;

import com.hoaxify.ws.user.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

/**
 * Created By Yasin Memic on May, 2020
 */
@Service
public class HoaxSecurityService {

    @Autowired
    HoaxRepository hoaxRepository;

    public boolean isAllowedToDelete(Long id, User loggedInUser) {
        Optional<Hoax> inDb = hoaxRepository.findById(id);
        if (!inDb.isPresent()) {
            return false;
        }
        Hoax hoax = inDb.get();
        if (hoax.getUser().getId() != loggedInUser.getId()) {
            return false;
        }
        return true;
    }
}