package com.pafable.javaDiscordBot.commands;

import net.dv8tion.jda.api.entities.channel.middleman.MessageChannel;
import net.dv8tion.jda.api.events.interaction.command.SlashCommandInteractionEvent;
import net.dv8tion.jda.api.interactions.commands.build.Commands;
import net.dv8tion.jda.api.interactions.commands.build.SlashCommandData;

public class Caution {
    private final static String caution =
            "The emperor protects, but it never hurts to check.";

    public static void createCommand(String command, SlashCommandInteractionEvent event) {
        if (command.equals("caution")) {
            MessageChannel channel = event.getChannel();

            channel.sendMessage(caution)
                    .submit();

            event.reply("Load you bolt gun")
                    .setEphemeral(true)
                    .queue();
        }
    }

    public final static SlashCommandData commandData = Commands.slash(
            "caution",
            "Display caution"
    );
}