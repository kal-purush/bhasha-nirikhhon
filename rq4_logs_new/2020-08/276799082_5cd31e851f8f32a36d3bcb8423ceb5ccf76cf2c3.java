package edu.pdx.cs.commands;

import edu.pdx.cs.Client;
import edu.pdx.cs.ClientState;
import org.apache.commons.net.ftp.FTPFile;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UploadCommand extends BaseCommand {

    public UploadCommand(Client client, List<String> arguments) {
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
            state.output("Uploading File...");
            String filename = arguments.get(0);
            return client.uploadFile(filename);
        }
        catch (IOException e)
        {
            return false;
        }
    }
}