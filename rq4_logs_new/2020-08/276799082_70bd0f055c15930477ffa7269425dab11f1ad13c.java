package edu.pdx.cs.commands;

import edu.pdx.cs.Client;
import edu.pdx.cs.ClientState;
import org.apache.commons.net.ftp.FTPFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ListLocalFilesCommand extends BaseCommand {


    public ListLocalFilesCommand(Client client, List<String> arguments) {
        super(client, arguments);
        expectNSubarguments(0);
    }

    @Override
    public boolean execute(ClientState state) {

        state.output("Retrieving file information...");
        ArrayList<String> fileNames = client.listFilesLocal();

        for (String fileName : fileNames) {
            state.output(fileName);
        }
        return true;

    }
}