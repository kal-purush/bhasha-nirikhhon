package bankClient;
import bankModel.PrivatBank;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;

public class PrivatBankClient {
    private final String PRIVAT_URI = "https://api.privatbank.ua/p24api/pubinfo?json&exchange&coursid=5";
    private final HttpClient client = HttpClient.newHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();

    public List<PrivatBank> getPrivatBank() {
        try {
            HttpRequest request = HttpRequest.newBuilder(new URI(PRIVAT_URI))
                    .GET()
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            return objectMapper.readValue(response.body(), new TypeReference<>() {
            });

        } catch (URISyntaxException e) {
            System.out.println("PRIVAT URI SYNTAX EXCEPTION!");
            throw new RuntimeException(e);
        } catch (JsonMappingException e) {
            System.out.println("PRIVAT Json Mapping Exception!");
            throw new RuntimeException(e);
        } catch (IOException e) {
            System.out.println("PRIVAT IOException!");
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            System.out.println("PRIVAT Interrupted Exception");
            throw new RuntimeException(e);
        }
    }
}