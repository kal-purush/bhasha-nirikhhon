package de.romjaki.discordrcon;

import net.dv8tion.jda.core.events.message.MessageReceivedEvent;

import java.awt.*;
import java.io.IOException;

import static de.romjaki.discordrcon.Util.*;

public class AddUserCommand implements Command {
    @Override
    public void execute(MessageReceivedEvent event, String[] args) {
        String name = args[1];
        String oldAccount = UserMapping.replaceMinecraftUserName(event.getAuthor(), name);
        if (name.equals(oldAccount)) {
            sendEmbed(event.getChannel(), "You already registered that minecraft account",
                    "You have already associated that minecraft account with your discord user",
                    Color.RED, event.getAuthor());
            return;
        }
        if (oldAccount != null) {
            if (UserMapping.isInUse(oldAccount)) {
                sendEmbed(event.getChannel(), "Your old minecraft account is still in use.",
                        String.format("Your old minecraft account `%s` is still in use " +
                                "and will stay whitelisted.", oldAccount), Color.BLUE, event.getAuthor());
            } else {
                try {
                    whitelist("remove", oldAccount);
                    sendEmbed(event.getChannel(), "Replacing minecraft account",
                            String.format("Your old minecraft account `%s` will be removed", oldAccount),
                            Color.blue, event.getAuthor());
                } catch (IOException e) {
                    showIOErrorMessage(event.getChannel(), event.getAuthor());
                    e.printStackTrace();
                }
            }
        }
        try {
            whitelist("add", name);
            sendEmbed(event.getChannel(), "Your minecraft account has whitelisted",
                    String.format("Your minecraft account `%s` has been whitelisted.", name),
                    Color.GREEN, event.getAuthor());
        } catch (IOException e) {
            showIOErrorMessage(event.getChannel(), event.getAuthor());
            e.printStackTrace();
        }
    }

    @Override
    public boolean requiresAdmin() {
        return false;
    }

    @Override
    public boolean requiresAdminOrSelfInvite() {
        return true;
    }


}