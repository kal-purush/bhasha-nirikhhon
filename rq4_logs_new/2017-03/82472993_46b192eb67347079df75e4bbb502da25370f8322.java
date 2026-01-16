package com.kvteam.backend.services;

import com.kvteam.backend.websockets.IPlayerConnection;
import org.springframework.stereotype.Service;

import java.io.IOException;

/**
 * Created by maxim on 26.03.17.
 */
@Service
public class GameService {

    public void startGame(IPlayerConnection attacker, IPlayerConnection defender){
        try {
            attacker.send("Game started with " + defender.getUsername());
            defender.send("Game started with " + attacker.getUsername());
            attacker.forceClose();
            defender.forceClose();
        } catch (IOException ignored) {

        }
    }

}