package cooper.parser;

import com.dopsun.chatbot.cli.Argument;
import com.dopsun.chatbot.cli.ParseResult;
import com.dopsun.chatbot.cli.Parser;
import cooper.exceptions.InvalidCommandFormatException;
import cooper.exceptions.InvalidUserRoleException;
import cooper.exceptions.UnrecognisedCommandException;
import cooper.ui.Ui;
import cooper.util.Util;
import cooper.verification.Login;
import cooper.verification.PasswordHasher;
import cooper.verification.Registration;
import cooper.verification.SignInDetails;
import cooper.verification.SignInProtocol;
import cooper.verification.UserRole;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public class SignInDetailsParser extends  ParserBase {

    private static SignInDetailsParser signInDetailsParserImpl = null;
    private Parser parser;

    /**
     * Constructor. Initialise internal parser.
     */
    private SignInDetailsParser()  {
        super();

        try {
            InputStream commandSetInputStream = this.getClass().getResourceAsStream("/parser/command-data.properties");

            File commandSetTmpFile = Util.inputStreamToTmpFile(commandSetInputStream,
                    System.getProperty("user.dir") + "/tmp", "/tmp_file_command.txt");

            InputStream trainingPathInputStream = this.getClass().getResourceAsStream("/parser/training-data.yml");
            File trainingTmpFile = Util.inputStreamToTmpFile(trainingPathInputStream,
                    System.getProperty("user.dir") + "/tmp", "/tmp_file_training.txt");

            parser = prepareParser(commandSetTmpFile.getPath(), trainingTmpFile.getPath());

        } catch (IOException | URISyntaxException e) {
            Ui.showText("Error encountered when creating temp file: "
                    + System.getProperty("user.dir") + "/tmp" + "/tmp_file_command.txt" + " or "
                    + System.getProperty("user.dir") + "/tmp" + "/tmp_file_training.txt");
        }
    }

    /**
     * API to parse a command in string.
     * @param input command to be parsed
     * @return a command object, to be passed into command handler
     */
    public static SignInProtocol parse(String input) throws UnrecognisedCommandException, NoSuchElementException,
            InvalidCommandFormatException, InvalidUserRoleException {
        if (signInDetailsParserImpl == null) {
            signInDetailsParserImpl = new SignInDetailsParser();
        }
        assert signInDetailsParserImpl != null;
        return signInDetailsParserImpl.parseInput(input);
    }

    public static String parseRawPassword(String input) throws InvalidCommandFormatException {
        if (signInDetailsParserImpl == null) {
            signInDetailsParserImpl = new SignInDetailsParser();
        }
        return signInDetailsParserImpl.parsePassword(input);
    }

    public SignInProtocol parseInput(String input) throws InvalidUserRoleException, UnrecognisedCommandException,
            InvalidCommandFormatException {
        assert input != null;
        String signInProtocol = input.split("\\s+")[0].toLowerCase();

        switch (signInProtocol) {
        case "login":
        case "register":
            return parseSignInDetailsInternal(input);
        default:
            throw new UnrecognisedCommandException();
        }
    }

    private SignInProtocol parseSignInDetailsInternal(String input) throws UnrecognisedCommandException,
            InvalidUserRoleException, NoSuchElementException, InvalidCommandFormatException {
        Optional<ParseResult> optResult = parser.tryParse(input);
        if (optResult.isPresent()) {
            var result = optResult.get();
            String command = result.allCommands().get(0).name();
            List<Argument> commandArgs = result.allCommands().get(0).arguments();
            switch (command) {
            case "login":
                SignInDetails signInDetails = parseSignInArgs(commandArgs);
                return new Login(signInDetails);
            case "register":
                signInDetails = parseSignInArgs(commandArgs);
                return new Registration(signInDetails);
            default:
                throw new UnrecognisedCommandException();
            }
        } else {
            throw new InvalidCommandFormatException();
        }
    }

    private SignInDetails parseSignInArgs(List<Argument> commandArgs) throws InvalidUserRoleException,
            NoSuchElementException, InvalidCommandFormatException {
        String username = null;
        String userEncryptedPassword = null;
        String userSalt = null;
        UserRole userRole = null;

        for (Argument a : commandArgs) {
            String argName = a.name();
            String argVal = a.value().get();
            switch (argName) {
            case "username-hint":
                username = argVal;
                break;
            case "password-hint":
                userSalt = PasswordHasher.getNewSalt();
                userEncryptedPassword = PasswordHasher.generatePasswordHash(argVal, userSalt);
                break;
            case "role-hint":
                if (argVal.equals("admin")) {
                    userRole = UserRole.ADMIN;
                } else if (argVal.equals("employee")) {
                    userRole = UserRole.EMPLOYEE;
                } else {
                    throw new InvalidUserRoleException();
                }
                break;
            default:
                throw new InvalidCommandFormatException();
            }
        }
        return new SignInDetails(username, userEncryptedPassword, userSalt, userRole);
    }

    private String parsePassword(String input) throws InvalidCommandFormatException {
        Optional<ParseResult> optResult = parser.tryParse(input);
        if (optResult.isPresent()) {
            var result = optResult.get();
            List<Argument> commandArgs = result.allCommands().get(0).arguments();
            return getRawPassword(commandArgs);
        } else {
            throw new InvalidCommandFormatException();
        }
    }

    private String getRawPassword(List<Argument> commandArgs) throws InvalidCommandFormatException {
        String userRawPassword = null;
        for (Argument a : commandArgs) {
            String argName = a.name();
            String argVal = a.value().get();
            switch (argName) {
            case "username-hint":
            case "role-hint":
                break;
            case "password-hint":
                userRawPassword = argVal;
                break;
            default:
                throw new InvalidCommandFormatException();
            }
        }
        return userRawPassword;
    }
}