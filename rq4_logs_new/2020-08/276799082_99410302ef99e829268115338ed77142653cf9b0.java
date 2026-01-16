package edu.pdx.cs.commands;

import edu.pdx.cs.Client;
import edu.pdx.cs.ClientState;

import java.util.ArrayList;
import java.util.List;

public class ListLocalDirCommand extends BaseCommand {

    public ListLocalDirCommand(Client client, List<String> arguments) {
        super(client, arguments);
        expectNSubarguments(0);
    }

    @Override
    public boolean execute(ClientState state) {
        state.output("Retrieving directory information...");
        ArrayList<String> dirNames = client.listDirectoriesLocal();

        for (String dirName : dirNames) {
            state.output(dirName);
        }
        return true;

    }
}