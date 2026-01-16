package app;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import modelos.User;

public class AppExemploAPI {

    public static void main(String[] args) throws Exception {
        
        URL minha_api = new URL("http://localhost:3000/student-info");
        URLConnection conn = minha_api.openConnection();
        BufferedReader in = new BufferedReader(
                                new InputStreamReader(
                                conn.getInputStream()));
        String inputLine;
        String buffer = "";

        while ((inputLine = in.readLine()) != null) {
            buffer += inputLine;
        }
        in.close();
        
        ObjectMapper mapper = new ObjectMapper();
        User colega = mapper.readValue(buffer, User.class);
        
        System.out.println("Nome do usuário = " + colega.getNome());
        System.out.println("Faculdade = " + colega.getFaculdade());
        System.out.println("Aula preferida = " + colega.getAula());
        System.out.println("Professor = " + colega.getProfessor());
        
    }
        
}