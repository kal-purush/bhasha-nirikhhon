package aula9;

import java.util.*;

public class Exercicio1 {

    public static void main(String[] args) {
        Map<Integer, String> loteria = new HashMap<>();
        loteria.put(0, "Ovos"); // valores com chaves repetidas serão sobreescritos
        loteria.put(0, "Ovos");
        loteria.put(0, "Ovos");
        loteria.put(1, "Água");
        loteria.put(2, "Escopeta");
        loteria.put(3, "Cavalo");
        loteria.put(4, "Dentista");
        loteria.put(5, "Fogo");

        for (int i = 0; i < loteria.size(); i++) {
            if (i < 5) {
                System.out.println("Loteria dos sonhos: " + loteria.get(i));
            }
        }

        //Criação do dicionario
        Map<String, List<String>> apelidos = new HashMap<>();

        //Lista para dicionario
        List<String> apelidosTairo = new ArrayList<>();
        apelidosTairo.add("Magrelo");
        apelidosTairo.add("Tairone");

        //Colocar lista em dicionario
        apelidos.put("Tairo", apelidosTairo);

        for (String chave : apelidos.keySet()) { //percorro a lista de chaves
            for (String nickName : apelidos.get(chave)) { // percorro a lista de valores
                System.out.println("Apelido: " + nickName);
            }
        }

        //Set alerta quando há valores repetidos
        Set<Integer> numeros = new HashSet<>();
        numeros.add(1);
        numeros.add(10);
        numeros.add(15);
        int soma = 0;

        for (Integer numero : numeros) {
            soma += numero;
        }

        System.out.println("Soma: " + soma);
    }
}