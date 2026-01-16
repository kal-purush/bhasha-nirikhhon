package org.endeavourhealth.imapi.controllers;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.endeavourhealth.imapi.logic.service.UserService;
import org.endeavourhealth.imapi.model.dto.RecentActivityItemDto;
import org.endeavourhealth.imapi.model.dto.ThemeDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.annotation.RequestScope;

import java.util.List;

@RestController
@RequestMapping("api/public/user")
@CrossOrigin(origins = "*")
@Tag(name = "UserController")
@RequestScope
public class UserController {
    private static final Logger LOG = LoggerFactory.getLogger(UserController.class);
    private final UserService userService = new UserService();

    @GetMapping(value = "/{user}/theme", produces = "application/json")
    public String getUserTheme(@PathVariable String user) {
        LOG.debug("getUserTheme");
        return userService.getUserTheme(user);
    }

    @PostMapping(value = "/{user}/theme", produces = "application/json")
    public ResponseEntity<String> updateUserTheme(@PathVariable String user, @RequestBody ThemeDto themeDto) throws JsonProcessingException {
        LOG.debug("updateUserTheme");
        userService.updateUserTheme(user, themeDto.getThemeValue());
        return ResponseEntity.ok().build();
    }

    @GetMapping(value = "/{user}/MRU", produces = "application/json")
    public List<RecentActivityItemDto> getUserMRU(@PathVariable String user) throws JsonProcessingException {
        LOG.debug("getUserMRU");
        return userService.getUserMRU(user);
    }

    @PostMapping(value = "/{user}/MRU", produces = "application/json")
    public ResponseEntity<String> updateUserMRU(@PathVariable String user, @RequestBody List<RecentActivityItemDto> mru) throws JsonProcessingException {
        LOG.debug("updateUserMRU");
        userService.updateUserMRU(user, mru);
        return ResponseEntity.ok().build();
    }

    @GetMapping(value = "/{user}/favourites", produces = "application/json")
    public List<String> getUserFavourites(@PathVariable String user) throws JsonProcessingException {
        LOG.debug("getUserFavourites");
        return userService.getUserFavourites(user);
    }

    @PostMapping(value = "/{user}/favourites", produces = "application/json")
    public ResponseEntity<String> updateUserFavourites(@PathVariable String user, @RequestBody List<String> favourites) throws JsonProcessingException {
        LOG.debug("updateUserFavourites");
        userService.updateUserFavourites(user, favourites);
        return ResponseEntity.ok().build();
    }
}