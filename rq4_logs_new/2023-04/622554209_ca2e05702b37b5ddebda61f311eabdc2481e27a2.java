package com.pafable.javaDiscordBot;

import com.pafable.javaDiscordBot.listeners.EventListener;
import net.dv8tion.jda.api.OnlineStatus;
import net.dv8tion.jda.api.entities.Activity;
import net.dv8tion.jda.api.sharding.DefaultShardManagerBuilder;
import net.dv8tion.jda.api.sharding.ShardManager;

import javax.security.auth.login.LoginException;

public class JavaDiscordBot {
    private final String token = System.getenv("DISCORD_BOT_TOKEN");
    private final ShardManager shardManager;

    public JavaDiscordBot() throws LoginException {

        // creates a shard aka instance of the bot
        DefaultShardManagerBuilder builder =
                DefaultShardManagerBuilder.createDefault(token);

        builder.setStatus(OnlineStatus.DO_NOT_DISTURB);

        builder.setActivity(
                Activity.watching("FOR THE EMPEROR!")
        );

        shardManager = builder.build();

        // register new listeners
        shardManager.addEventListener(new EventListener());
    }

    public ShardManager getShardManager() {
        return shardManager;
    }

    public static void main(String[] args) {
        try {
            JavaDiscordBot bot = new JavaDiscordBot();
        } catch (LoginException e) {
            System.out.println("ERROR: Bot token is invalid");
        }
    }
}