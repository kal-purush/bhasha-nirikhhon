package edu.pdx.cs.commands;

import edu.pdx.cs.Client;
import edu.pdx.cs.ClientState;
import org.apache.commons.net.ftp.FTPFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ListRemoteDirCommand extends BaseCommand {

    public ListRemoteDirCommand(Client client, List<String> arguments) {
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
            state.output("Retrieving directory information...");
            FTPFile[] dirs = client.listRemoteDirectories();
            ArrayList<String> dirNames = client.fileDirectoryListStrings(dirs);

            for (String dirName : dirNames) {
                //state.output(dirName);
                System.out.println(dirName);
            }
            return true;
        }
        catch (IOException e)
        {
            return false;
        }
    }
}