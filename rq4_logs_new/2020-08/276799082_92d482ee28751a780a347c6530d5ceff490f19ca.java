package edu.pdx.cs.commands;

import edu.pdx.cs.Client;
import edu.pdx.cs.ClientState;

import java.io.IOException;
import java.util.List;

public class DownloadCommand extends BaseCommand {

    public DownloadCommand(Client client, List<String> arguments) {
        super(client, arguments);
        expectNSubarguments(1);
    }

    @Override
    public boolean execute(ClientState state)
    {
        if (!state.getLoggedIn()) {
            state.output("User is not logged in. Please log in to continue.");
        }
        try
        {
            state.output("Downloading File...");
            String filename = arguments.get(0);
            return client.retrieveFile(filename);
        }
        catch (IOException e)
        {
            return false;
        }
    }
}