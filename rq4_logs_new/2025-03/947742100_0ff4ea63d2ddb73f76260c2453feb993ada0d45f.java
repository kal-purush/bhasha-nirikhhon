package framework;

import lombok.Getter;

import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;

@Getter
public class URL {

    private String url;
    private String target;
    private Map<String, String> params = new HashMap<>();

    public URL(String url) throws MalformedURLException {
        this.url = url;
        parseUrl(url);
    }

    private void parseUrl(String url) throws MalformedURLException {
        if (!url.startsWith("/")) {
            throw new MalformedURLException(url + " not found");
        }

        try {
            String[] tokens = url.split("[?=&]");
            target = tokens[0];
            for (int i = 1; i < tokens.length; i++) {
                params.put(tokens[i], tokens[++i]);
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new MalformedURLException(url + " not found");
        }
    }
}