package com.pafable.javaDiscordBot.builders;

import net.dv8tion.jda.api.OnlineStatus;
import net.dv8tion.jda.api.entities.Activity;
import net.dv8tion.jda.api.requests.GatewayIntent;
import net.dv8tion.jda.api.sharding.DefaultShardManagerBuilder;
import net.dv8tion.jda.api.sharding.ShardManager;

import java.util.Arrays;

public class EventBuilder {
    public static ShardManager createShardManger(String token, GatewayIntent... intents) {
        // creates a shard aka instance of the bot
        DefaultShardManagerBuilder builder =
                DefaultShardManagerBuilder.createDefault(token);

        builder.setStatus(OnlineStatus.DO_NOT_DISTURB);

        builder.setActivity(
                Activity.of(
                        Activity.ActivityType.LISTENING,
                        "to the Emperor of Mankind!"
                )
        );

        // enable intents
        builder.enableIntents(
                Arrays.stream(intents).toList()
        );

        // caches user
        return builder.build();
    }
}