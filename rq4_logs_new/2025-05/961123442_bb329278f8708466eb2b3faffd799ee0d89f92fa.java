package visao;

import modelo.Ator;

import java.util.List;
import java.util.Scanner;

public class VisaoAtores {

    // Leitura dos dados do ator (usado no cadastro e atualização)
    public Ator leAtor(Scanner sc) {
        System.out.println("----- Cadastro/Atualização de Ator -----");
        System.out.print("Nome do ator: ");
        String nome = sc.nextLine();

        if (nome.trim().isEmpty()) {
            System.out.println("❌ Nome inválido.");
            return null;
        }

        return new Ator(nome.trim());
    }

    // Exibição dos dados do ator
    public void mostraAtor(Ator a) {
        System.out.println("\n-----------------------------");
        System.out.println("ID: " + a.getID());
        System.out.println("Nome: " + a.getNome());
    }

    // Exibe os nomes das séries vinculadas ao ator
    public void mostraSeriesVinculadas(List<String> nomesSeries) {
        if (nomesSeries == null || nomesSeries.isEmpty()) {
            System.out.println("Séries: (nenhuma série vinculada)");
        } else {
            System.out.println("Séries:");
            for (String nome : nomesSeries) {
                System.out.println("- " + nome);
            }
        }
    }
}