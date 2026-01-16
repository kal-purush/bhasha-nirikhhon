package com.stefanolupo.ndngame.backend;

import com.stefanolupo.ndngame.backend.publisher.PlayerStatusPublisher;
import com.stefanolupo.ndngame.protos.PlayerStatus;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class LocalPlayerReference {

    private final PlayerStatusPublisher playerStatusPublisher;
    private PlayerStatus playerStatus;

    @Inject
    public LocalPlayerReference(PlayerStatusPublisher playerStatusPublisher) {
        this.playerStatusPublisher = playerStatusPublisher;
        playerStatus = PlayerStatus.getDefaultInstance();
    }

    public void setPlayerStatus(PlayerStatus playerStatus) {
         this.playerStatus = playerStatus;
         playerStatusPublisher.updateLocalPlayerStatus(playerStatus);
    }

    public PlayerStatus getPlayerStatus() {
        return playerStatus;
    }
}