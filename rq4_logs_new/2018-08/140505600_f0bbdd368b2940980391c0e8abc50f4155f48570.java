package AuthClient;

import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.auth.oauth2.GoogleTokenResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;

import java.io.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class OAuth2Client {

    private static final String CREDENTIAL_FILE_PATH = "credentials.token";
    private static final String SCOPE = "https://www.googleapis.com/auth/blogger";

    private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
    static final JsonFactory JSON_FACTORY = new JacksonFactory();

    private static GoogleAuthorizationCodeFlow initializeFlow() throws IOException {
        return new GoogleAuthorizationCodeFlow.Builder(
                new NetHttpTransport(), JacksonFactory.getDefaultInstance(),
                OAuth2ClientCredentials.CLIENT_ID, OAuth2ClientCredentials.CLIENT_SECRET,
                Collections.singleton(SCOPE)).build();
    }

    // Based on: https://gitlab.tubit.tu-berlin.de/klems/googledrivejavaexample/blob/5688e4bc907424036a5b9857f33d3bd50b51b29e/src/com/google/drive/example/DriveCommandLine.java
    public static void authorize() {
        if (validCredentialsStored()) {
            System.out.println("Valid credentials stored. No need to re-authorize.");
            System.out.printf("You can also delete the file %s to force a re-auth.\n", CREDENTIAL_FILE_PATH);
            return;
        }

        GoogleAuthorizationCodeFlow flow = null;
        try {
            flow = initializeFlow();
        } catch (IOException e) {
            e.printStackTrace();
        }

        String url = flow.newAuthorizationUrl().setRedirectUri(OAuth2ClientCredentials.COPY_PASTE_REDIRECT_URI).build();
        System.out.println("Please open the following url in your browser:");
        System.out.println(url);
        System.out.println("Then type the authorization code:");

        Scanner input = new Scanner(System.in);
        String code = input.nextLine();

        GoogleTokenResponse response = null;
        try {
            response = flow.newTokenRequest(code).setRedirectUri(OAuth2ClientCredentials.COPY_PASTE_REDIRECT_URI).execute();
        } catch (IOException e) {
            e.printStackTrace();
        }
        GoogleCredential credential = (new GoogleCredential.Builder())
                .setJsonFactory(new JacksonFactory())
                .setTransport(HTTP_TRANSPORT)
                .setClientSecrets(OAuth2ClientCredentials.CLIENT_ID, OAuth2ClientCredentials.CLIENT_SECRET)
                .build().setFromTokenResponse(response);

        System.out.println("Authenticated correctly! :D");
        System.out.println(response);
        System.out.println(credential);

        OAuth2ClientCredentials.AccessToken = response.getAccessToken();
        OAuth2ClientCredentials.RefreshToken = response.getRefreshToken();
        OAuth2ClientCredentials.TokenObtainedMillis = System.currentTimeMillis();
        OAuth2ClientCredentials.TokenExpiresInMillis = System.currentTimeMillis() + response.getExpiresInSeconds() * 1000;

        System.out.println("Saving credentials to disk...");
        saveCredentials();
    }

    private static boolean validCredentialsStored() {
        File f = new File(CREDENTIAL_FILE_PATH);
        if (f.exists()) {
            try {
                FileInputStream fis = new FileInputStream(f);
                ObjectInputStream ois = new ObjectInputStream(fis);

                Map<String, String> keyMap = (Map<String, String>) ois.readObject();
                OAuth2ClientCredentials.AccessToken = keyMap.get("accessToken");
                OAuth2ClientCredentials.RefreshToken = keyMap.get("refreshToken");
                OAuth2ClientCredentials.TokenExpiresInMillis = Long.parseLong(keyMap.get("expiresInMillis"));
                OAuth2ClientCredentials.TokenObtainedMillis = Long.parseLong(keyMap.get("obtainsInMillis"));

                if (System.currentTimeMillis() > OAuth2ClientCredentials.TokenExpiresInMillis) {
                    System.out.println("Access Token Expired...");
                    return false;
                }
                return true;
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

        }
        return false;
    }

    private static void saveCredentials() {
        try {
            FileOutputStream fos = new FileOutputStream(CREDENTIAL_FILE_PATH);
            Map<String, String> keyMap = new HashMap<String, String>();
            keyMap.put("accessToken", OAuth2ClientCredentials.AccessToken);
            keyMap.put("refreshToken", OAuth2ClientCredentials.RefreshToken);
            keyMap.put("expiresInMillis", Long.toString(OAuth2ClientCredentials.TokenExpiresInMillis));
            keyMap.put("obtainsInMillis", Long.toString(OAuth2ClientCredentials.TokenObtainedMillis));
            ObjectOutputStream oos = new ObjectOutputStream(fos);

            oos.writeObject(keyMap);

            oos.close();
            fos.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        // Run this to get the access_token and refresh_token.
        // Follow instructions on console.
        OAuth2Client.authorize();
    }
}