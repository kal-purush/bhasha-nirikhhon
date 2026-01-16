package com.jagrosh.vortex.commands;

import com.jagrosh.jdautilities.command.CommandClient;
import com.jagrosh.jdautilities.command.CommandEvent;
import com.jagrosh.jdautilities.command.SlashCommandEvent;
import com.jagrosh.vortex.Vortex;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import net.dv8tion.jda.api.JDA;
import net.dv8tion.jda.api.Permission;
import net.dv8tion.jda.api.entities.Guild;
import net.dv8tion.jda.api.entities.Member;
import net.dv8tion.jda.api.entities.User;
import net.dv8tion.jda.api.utils.messages.MessageCreateData;

/**
 * Tries to unify slash and regular commands by providing a common wrapper
 */
public sealed interface HybridEvent permits HybridEvent.SlashHybridEvent, HybridEvent.TextHybridEvent {
    void reply(String msg);
    void reply(MessageCreateData data);
    void replyError(MessageCreateData data);
    void replyError(String msg);
    User getUser();
    Member getMember();
    Guild getGuild();
    default JDA getJDA() {
        return getUser().getJDA();
    }

    SlashCommandEvent getSlashCommandEvent();

    CommandEvent getCommandEvent();
    boolean isSlashCommandEvent();

    boolean hasGeneralCommandPerms(Vortex vortex, Permission... altPerms);

    CommandClient getClient();
    static HybridEvent of(SlashCommandEvent e) {
        return new SlashHybridEvent(e);
    }

    static HybridEvent of(CommandEvent e) {
        return new TextHybridEvent(e);
    }

    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    non-sealed class SlashHybridEvent implements HybridEvent {
        private SlashCommandEvent e;

        @Override
        public void reply(String msg) {
            e.reply(msg).queue();
        }

        @Override
        public void reply(MessageCreateData data) {
            e.reply(data).queue();
        }

        @Override
        public void replyError(MessageCreateData data) {
            e.reply(data).setEphemeral(true).queue();
        }

        @Override
        public void replyError(String msg) {
            e.reply(msg).setEphemeral(true).queue();
        }

        @Override
        public User getUser() {
            return e.getUser();
        }

        @Override
        public Member getMember() {
            return e.getMember();
        }

        @Override
        public Guild getGuild() {
            return e.getGuild();
        }

        @Override
        public SlashCommandEvent getSlashCommandEvent() {
            return e;
        }

        @Override
        public CommandEvent getCommandEvent() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isSlashCommandEvent() {
            return true;
        }

        @Override
        public boolean hasGeneralCommandPerms(Vortex vortex, Permission... altPerms) {
            return CommandTools.hasGeneralCommandPerms(vortex, e, altPerms);
        }

        @Override
        public CommandClient getClient() {
            return e.getClient();
        }
    }

    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    non-sealed class TextHybridEvent implements HybridEvent {
        private CommandEvent e;
        @Override
        public void reply(String msg) {
            e.reply(msg);
        }

        @Override
        public void reply(MessageCreateData data) {
            e.reply(data);
        }

        @Override
        public void replyError(MessageCreateData data) {
            e.reply(data);
        }

        @Override
        public void replyError(String msg) {
            e.reply(msg);
        }

        @Override
        public User getUser() {
            return e.getAuthor();
        }

        @Override
        public Member getMember() {
            return e.getMember();
        }

        @Override
        public Guild getGuild() {
            return e.getGuild();
        }

        @Override
        public SlashCommandEvent getSlashCommandEvent() {
            throw new UnsupportedOperationException();
        }

        @Override
        public CommandEvent getCommandEvent() {
            return e;
        }

        @Override
        public boolean isSlashCommandEvent() {
            return false;
        }

        @Override
        public boolean hasGeneralCommandPerms(Vortex vortex, Permission... altPerms) {
            return CommandTools.hasGeneralCommandPerms(vortex, e, altPerms);
        }

        @Override
        public CommandClient getClient() {
            return e.getClient();
        }
    }
}