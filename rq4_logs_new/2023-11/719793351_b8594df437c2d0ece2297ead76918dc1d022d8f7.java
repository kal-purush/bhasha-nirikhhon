import br.com.alura.modelos.Endereco;

import br.com.alura.modelos.EnderecoApi;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {

        Gson gson = new GsonBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                .setPrettyPrinting()
                .create();

        Scanner leitor = new Scanner(System.in);

        List<Endereco> listaDeEnderecos = new ArrayList<>();

        int busca = 0;

        while (busca != -1) {

            System.out.println("Digite o CEP a buscar ou -1 para sair");
            busca = leitor.nextInt();

            if (busca == -1) {
                break;
            }

            String endereco = "https://viacep.com.br/ws/" + busca + "/json/";
            try {
                HttpClient cliente = HttpClient.newHttpClient();
                HttpRequest pedido = HttpRequest.newBuilder()
                        .uri(URI.create(endereco))
                        .build();

                HttpResponse<String> resposta = cliente
                        .send(pedido, HttpResponse.BodyHandlers.ofString());

                System.out.println(resposta.body());

                EnderecoApi xApi = gson.fromJson(resposta.body(), EnderecoApi.class);

                Endereco x = new Endereco(xApi);

                listaDeEnderecos.add(x);

                FileWriter escrita = new FileWriter(x.getCep() + ".json");
                escrita.write(gson.toJson(x));
                escrita.close();

            }catch (RuntimeException | IOException e) {
                System.out.println(e.getMessage());
                System.out.println("Finalizando a aplicação");
            }

        }
        System.out.println("Aplicação finalizada com um total de " + listaDeEnderecos.size() + " buscas: ");
        System.out.println(listaDeEnderecos);

    }
}