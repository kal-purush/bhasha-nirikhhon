package com.pafable.javaDiscordBot.commands;

import net.dv8tion.jda.api.entities.channel.middleman.MessageChannel;
import net.dv8tion.jda.api.events.interaction.command.SlashCommandInteractionEvent;
import net.dv8tion.jda.api.interactions.commands.build.Commands;
import net.dv8tion.jda.api.interactions.commands.build.SlashCommandData;

public class Motto {
    private final static String motto =
            "We are vigilance unending. "
                    + "We are duty unstinting. "
                    + "We are punishment inescapable. "
                    + "We are the Adeptus Custodes, and all must fear our wrath.";

    public static void createCommand(String command, SlashCommandInteractionEvent event) {
        if (command.equals("motto")) {
            MessageChannel channel = event.getChannel();

            channel.sendMessage(motto)
                    .queue();

            event.reply("Adeptus Custodes")
                    .setEphemeral(true)
                    .queue();
        }
    }

    public final static SlashCommandData commandData = Commands.slash(
            "motto",
            "Display server motto"
    );
}