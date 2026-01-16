package cooper.storage;

import cooper.exceptions.InvalidFileDataException;
import cooper.ui.Ui;
import cooper.verification.SignInDetails;
import cooper.verification.UserRole;
import cooper.verification.Verifier;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class SignInDetailsStorage extends Storage {

    public SignInDetailsStorage(String filePath) {
        super(filePath);
    }

    public void loadSignInDetails(Verifier cooperVerifier) {
        HashMap<String, SignInDetails> registeredUsers = cooperVerifier.getRegisteredUsers();
        Scanner fileScanner = getScanner(filePath);
        readSignInDetails(fileScanner, registeredUsers);
    }

    public void saveSignInDetails(Verifier cooperVerifier) {
        try {
            writeSignInDetails(filePath, cooperVerifier.getRegisteredUsers());
        } catch (IOException e) {
            System.out.println("Error writing to file: " + e.getMessage());
            System.exit(1);
        }
    }

    private static void readSignInDetails(Scanner fileScanner, HashMap<String, SignInDetails> registeredUsers) {
        if (fileScanner != null) {
            while (fileScanner.hasNext()) {
                String signInDetails = fileScanner.nextLine();
                try {
                    SignInDetails decodedSignInDetails = decodeSignInDetails(signInDetails);
                    registeredUsers.put(decodedSignInDetails.getUsername(), decodedSignInDetails);
                } catch (InvalidFileDataException e) {
                    Ui.showInvalidFileDataError();
                }
            }
        }
    }

    private static SignInDetails decodeSignInDetails(String signInDetailsAsString) throws InvalidFileDataException {
        String[] signInDetails = signInDetailsAsString.split("\\|");
        if (isInvalidFileData(signInDetails)) {
            throw new InvalidFileDataException();
        }
        assert !isInvalidFileData(signInDetails);

        String username = signInDetails[0].trim();
        String userEncryptedPassword = signInDetails[1].trim();
        String userSalt = signInDetails[2].trim();
        UserRole userRole = signInDetails[3].trim().equals("A") ? UserRole.ADMIN : UserRole.EMPLOYEE;

        return new SignInDetails(username, userEncryptedPassword, userSalt, userRole);
    }

    private static boolean isInvalidFileData(String[] signInDetails) {
        if (signInDetails.length != 4) {
            return true;
        }

        if (!signInDetails[3].trim().equals("A") && !signInDetails[3].equals("E")) {
            return true;
        }

        for (String s : signInDetails) {
            if (s.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    private static void writeSignInDetails(Path filePath, HashMap<String, SignInDetails> registeredUsers)
            throws IOException {
        FileWriter fileWriter = new FileWriter(filePath.toString(), false);

        for (Map.Entry<String, SignInDetails> e : registeredUsers.entrySet()) {
            String encodedSignInDetails = encodeSignInDetails(e);
            fileWriter.write(encodedSignInDetails + System.lineSeparator());
        }
        fileWriter.close();
    }

    private static String encodeSignInDetails(Map.Entry<String, SignInDetails> registeredUser) {
        StringBuilder encodedSignInDetails = new StringBuilder();

        String username = registeredUser.getKey();
        encodedSignInDetails.append(username).append(" | ");

        String userEncryptedPassword = registeredUser.getValue().getUserEncryptedPassword();
        encodedSignInDetails.append(userEncryptedPassword).append(" | ");

        String userSalt = registeredUser.getValue().getUserSalt();
        encodedSignInDetails.append(userSalt).append(" | ");

        String userRole = registeredUser.getValue().getUserRole().equals(UserRole.ADMIN) ? "A" : "E";
        encodedSignInDetails.append(userRole);

        return String.valueOf(encodedSignInDetails);
    }
}
