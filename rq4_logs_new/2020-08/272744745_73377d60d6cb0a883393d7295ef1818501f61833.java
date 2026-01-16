package com.dungeongroupfinder.controllers;

import com.dungeongroupfinder.helpers.HelperClass;
import com.dungeongroupfinder.objects.PendingList;
import com.dungeongroupfinder.objects.PendingMember;
import com.dungeongroupfinder.security.PlayerDetails;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.security.Principal;
import java.util.List;

@RestController
@RequestMapping("pending")
public class PendingListController {

    @Autowired
    private PendingList pendingList;

    @GetMapping
    public List<PendingMember> getPendingList() {
        return pendingList.getPendingList();
    }

    @GetMapping("/{guildId}")
    public List<PendingMember> getPendingListForGuildId(@PathVariable int guildId) {
        return pendingList.getPendingListForGuildId(guildId);
    }

    @ResponseStatus(HttpStatus.ACCEPTED)
    @PostMapping
    public void addToPendingList(@RequestBody int guildId, Principal principal) {
        PlayerDetails playerDetails = HelperClass.castToPlayerDetails(principal);
        pendingList.addToPendingList(guildId, playerDetails);
    }

    @ResponseStatus(HttpStatus.NO_CONTENT)
    @DeleteMapping
    public void deleteFromPendingList(@RequestBody int playerId, Principal principal) {
        PlayerDetails playerDetails = HelperClass.castToPlayerDetails(principal);
        pendingList.deleteFromPendingList(playerId, playerDetails);
    }

    @ResponseStatus(HttpStatus.ACCEPTED)
    @PostMapping("{guildId}/approve/{playerId}")
    public void approveMember(@PathVariable int guildId, @PathVariable int playerId,
                              Principal principal) {
        PlayerDetails playerDetails = HelperClass.castToPlayerDetails(principal);
        pendingList.approveMember(guildId, playerId, playerDetails);
    }

}