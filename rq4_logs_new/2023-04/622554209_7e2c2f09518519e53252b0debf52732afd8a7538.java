package com.pafable.javaDiscordBot.commands;

import net.dv8tion.jda.api.entities.Role;
import net.dv8tion.jda.api.events.interaction.command.SlashCommandInteractionEvent;
import net.dv8tion.jda.api.interactions.commands.build.Commands;
import net.dv8tion.jda.api.interactions.commands.build.SlashCommandData;

import java.util.Objects;

public class Roles {
    public static void createCommand(String command, SlashCommandInteractionEvent event) {
        if (command.equals("roles")) {
            event.deferReply()
                    .queue();

            StringBuilder response = new StringBuilder();

            for (Role role : Objects.requireNonNull(event.getGuild()).getRoles()) {
                response.append(role.getAsMention()).append("\n");
            }

            event.getHook()
                    .sendMessage(response.toString())
                    .queue();
        }
    }

    public static final SlashCommandData commandData = Commands.slash(
            "roles",
            "Display all roles"
    );
}