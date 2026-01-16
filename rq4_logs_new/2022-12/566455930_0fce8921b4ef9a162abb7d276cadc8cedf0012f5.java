package org.jkube.gitbeaver.security.commands;

import org.jkube.gitbeaver.AbstractCommand;
import org.jkube.gitbeaver.GitBeaver;
import org.jkube.gitbeaver.WorkSpace;
import org.jkube.gitbeaver.interfaces.Command;
import org.jkube.gitbeaver.security.SecretType;
import org.jkube.gitbeaver.security.SecurityManagement;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Usage: git clone providerUrl repositoryName [tag]
 */
public class WithSecretInCommand extends AbstractCommand {

    private static final String SECRET_FILE_VARIABLE = "secret";

    public WithSecretInCommand() {
        super(4, null, "with", "secret");
    }

    @Override
    public void execute(Map<String, String> variables, WorkSpace workSpace, List<String> arguments) {
        String secret = SecurityManagement.getSecret(arguments.get(0), SecretType.FILE);
        expectArg(1, "in", arguments);
        Path secretfile = SecurityManagement.createSecretFile(secret, arguments.get(2));
        List<String> called = arguments.subList(3, arguments.size());
        List<String> calledArguments = new ArrayList<>();
        Command command = GitBeaver.commandParser().parseCommand(called.toArray(new String[0]), calledArguments);
        command.execute(variables, workSpace, calledArguments);
        SecurityManagement.deleteSecretFile(secretfile);
    }

}