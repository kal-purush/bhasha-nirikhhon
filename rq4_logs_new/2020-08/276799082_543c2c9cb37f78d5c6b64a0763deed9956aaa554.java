package edu.pdx.cs.commands;

import edu.pdx.cs.Client;
import edu.pdx.cs.ClientState;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class LogoutCommand extends BaseCommand {

    public LogoutCommand(Client client, List<String> arguments)
    {
        super(client, arguments);
        expectNSubarguments(1);
    }

    @Override
    public boolean execute(ClientState state)
    {
        String username = arguments.get(0);
        if (username == null)
            throw new IllegalStateException("Attempted execution before assigning input.");
        try
        {
            boolean result = client.logout(username);
            state.setLoggedIn(!result);
            if (result)
            {
                state.output("Logged out of server as " + username + ".");
            }
            else
                state.output("Failed to logout of server with username " + username + ".");

            return result;
        }
        catch (IOException e)
        {
            return false;
        }
    }
}