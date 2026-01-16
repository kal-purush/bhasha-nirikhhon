package ui;

import java.io.IOException;
import java.net.HttpURLConnection;

public class ExceptionReader {

    public String responseReader(HttpURLConnection http){
        try {
            if(http.getResponseCode() == 403){
                return "Username already taken!";
            }
            if(http.getResponseCode() == 400){
                return "Bad request- try again!";
            }
            if(http.getResponseCode() == 401){
                return "Unauthorized!";
            }
            else{
                return "Unknown error code";
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}