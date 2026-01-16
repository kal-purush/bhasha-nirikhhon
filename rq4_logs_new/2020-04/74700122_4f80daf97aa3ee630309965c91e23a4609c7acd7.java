package com.brohoof.brohoofbans;

import java.util.Optional;
import java.util.UUID;

public class API {

    private Data data;

    API(Data d) {
        this.data = d;
    }

    public Optional<Ban> getBan(String playerName) {
        return data.getBan(playerName);
    }

    public Optional<Ban> getBan(UUID playerUUID) {
        return data.getBan(playerUUID);
    }

    public boolean isBanned(String playerName) {
        return data.getBan(playerName).isPresent();
    }

    public boolean isBanned(UUID playerUUID) {
        return data.getBan(playerUUID).isPresent();
    }

    public void ban(Ban ban) {
        if (isBanned(ban.getVictim()))
            data.saveBan(ban);
        else
            data.createBan(ban);
    }

    public void unban(Ban ban) {
        data.unban(ban);
    }
}