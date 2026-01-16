package bankClient;

import bankModel.MonoBank;
import com.fasterxml.jackson.core.JsonProcessingException;
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
import java.util.SortedMap;

public class clientMonoBank {
    private final String MONO_URI = "https://api.monobank.ua/bank/currency";
    HttpClient client = HttpClient.newHttpClient();
    ObjectMapper objectMapper = new ObjectMapper();
    public List<MonoBank> getMonoBank(){
        try{
            HttpRequest request = HttpRequest.newBuilder(new URI(MONO_URI))
                    .GET()
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            return objectMapper.readValue(response.body(), new TypeReference<List<MonoBank>>() {
            });

        }catch (URISyntaxException e){
            System.out.println("MONO URI SYNTAX EXCEPTION!");
            throw new RuntimeException(e);
        } catch (JsonMappingException e) {
            System.out.println("MONO Json Mapping Exception!");
            throw new RuntimeException(e);
        } catch (IOException e) {
            System.out.println("MONO IOException!");
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            System.out.println("MONO Interrupted Exception");
            throw new RuntimeException(e);
        }
    }
}