package com.pafable.javaDiscordBot.commands;

import net.dv8tion.jda.api.entities.Role;
import net.dv8tion.jda.api.events.guild.GuildJoinEvent;
import net.dv8tion.jda.api.events.guild.GuildReadyEvent;
import net.dv8tion.jda.api.events.interaction.command.SlashCommandInteractionEvent;
import net.dv8tion.jda.api.hooks.ListenerAdapter;
import net.dv8tion.jda.api.interactions.commands.build.CommandData;
import net.dv8tion.jda.api.interactions.commands.build.Commands;
import net.dv8tion.jda.api.interactions.commands.build.SlashCommandData;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

public class CommandManager extends ListenerAdapter {

    private List<CommandData> commandData = new ArrayList<>();
    private final SlashCommandData welcomeCommand = Commands.slash(
            "welcome",
            "Get welcomed by bot"
    );

    @Override
    public void onSlashCommandInteraction(@NotNull SlashCommandInteractionEvent event) {
        super.onSlashCommandInteraction(event);

        String command = event.getName();
        if (command.equals("welcome")) {
            String userTag = event.getUser().getAsTag();

            event.deferReply()
                    .queue();

            event.getHook()
                    .sendMessage(
                            "Welcome to the server, **"
                            + userTag
                            + "**!"
                    ).queue();

        } else if (command.equals("roles")) {
            event.deferReply()
                    .queue();

            String response = "";

            for (Role role : event.getGuild().getRoles()) {
                response += role.getAsMention() + "\n";
            }

            event.getHook()
                    .sendMessage(response)
                    .queue();
        }
    }

    // Guild command -- instantly updated (max 100)
    @Override
    public void onGuildReady(GuildReadyEvent event) {
        super.onGuildReady(event);

        commandData.add(welcomeCommand);

        commandData.add(
                Commands.slash(
                    "roles",
                    "Display all roles"
                )
        );

        event.getGuild()
                .updateCommands()
                .addCommands(commandData)
                .queue();
    }

    @Override
    public void onGuildJoin(GuildJoinEvent event) {
        super.onGuildJoin(event);

        commandData.add(welcomeCommand);

        event.getGuild()
                .updateCommands()
                .addCommands(commandData)
                .queue();
    }
}