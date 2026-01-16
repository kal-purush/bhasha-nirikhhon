package com.gameder.app.handlers.profiles;

import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;

@RestController
public class ProfilesHandler {
    @CrossOrigin
    @RequestMapping(value = "/api/profiles", method = RequestMethod.GET)
    public ArrayList<Profile> getGamerList() {
        ArrayList<Profile> profiles = new ArrayList<Profile>();
        profiles.add(new Profile("A Little Pony", "http://cartoonbros.com/wp-content/uploads/2016/04/My-Little-Pony-9.png"));
        profiles.add(new Profile("Another Little Pony", "http://cartoonbros.com/wp-content/uploads/2016/04/My-Little-Pony-10.png"));
        profiles.add(new Profile("Yet Another Little Pony", "http://cartoonbros.com/wp-content/uploads/2016/04/My-Little-Pony-11.png"));

        return profiles;
    }
}