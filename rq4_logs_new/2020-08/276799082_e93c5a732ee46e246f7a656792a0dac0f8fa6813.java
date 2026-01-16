package edu.pdx.cs.commands;

import edu.pdx.cs.Client;
import edu.pdx.cs.ClientState;
import org.apache.commons.net.ftp.FTPFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ListRemoteFileCommand extends BaseCommand {

    public ListRemoteFileCommand(Client client, List<String> arguments) {
        super(client, arguments);
        expectNSubarguments(0);
    }

    @Override
    public boolean execute(ClientState state)
    {
        if (!state.getLoggedIn()) {
            state.output("User is not logged in. Please log in to continue.");
        }
        try
        {
            state.output("Retrieving file information...");
            FTPFile[] dirs = client.listRemoteFiles();
            ArrayList<String> fileNames = client.fileDirectoryListStrings(dirs);

            for (String fileName : fileNames) {
                state.output(fileName);
            }
            return true;
        }
        catch (IOException e)
        {
            return false;
        }
    }
}