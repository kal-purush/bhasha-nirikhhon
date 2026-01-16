package Control;

import Model.Cidadao;
import Model.Endereco;
import Model.Vacina;
import Model.Vacinacao;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class Add {

    static Scanner sc = new Scanner(System.in);
    static HashMap<Integer, Vacina> vacinas = new HashMap<>();
    static ArrayList<Vacinacao> vacinacoes = new ArrayList<>();
    static ArrayList<Endereco> enderecos = new ArrayList<>();

    public static Cidadao addCidadao() {

        System.out.print("Nome: ");
        String nome = sc.nextLine();
        System.out.print("Sexo: ");
        String sexo = sc.nextLine();
        System.out.print("Nacionalidade: ");
        String nacionalidade = sc.nextLine();
        System.out.print("Data de nascimento (dd/MM/aaaa): ");
        String dataNasc = sc.nextLine();
        System.out.print("Digite o peso: ");
        float peso = sc.nextFloat();
        System.out.print("Digite a altura: ");
        float altura = sc.nextFloat();
        System.out.println("");
        Cidadao cidadoes = new Cidadao(nome, sexo, nacionalidade, dataNasc, peso, altura, enderecos, vacinas);

        return cidadoes;

    }

    public static HashMap<Integer, Vacina> addVacina() {

        System.out.print("Digite o código da vacina: ");
        int codVacina = sc.nextInt();
        System.out.print("Tipo da vacina: ");
        String tipoVacina = sc.nextLine();
        System.out.print("Digite a periodicidade da vacina (anos): ");
        int periodicidade = sc.nextInt();
        System.out.print("Descrição: ");
        String desc = sc.nextLine();
        System.out.print("Observação: ");
        String observacao = sc.nextLine();
        System.out.print("Data da vacina formato (dd/MM/aaaa): ");
        String datVacina = sc.nextLine();
        vacinas.put(codVacina, new Vacina(codVacina, periodicidade, tipoVacina, desc, observacao, datVacina));

        return vacinas;

    }

    public static HashMap<Integer, Vacina> removeVacina() {

        return vacinas;
    }

    public static void mostraVacinas() {
        System.out.println("\n_____ Lista de vacinas cadastradas _____\n");
        for (Map.Entry<Integer, Vacina> entry : vacinas.entrySet()) {
            if (!vacinas.isEmpty()) {
                Integer key = entry.getKey();
                Vacina value = entry.getValue();
                System.out.println(value + "\n");
            } else {
                System.out.println("Nenhuma vacina cadastrada!");
                // NÃO ENTRA NESSE ELSE, obs: NÃO CONSEGUI DEPURAR
            }
        }

    }

    public static void mostraVacinacoes() {

        System.out.println("\n_____ Lista de vacinas tomadas _____\n");
        for (Vacinacao vacinacoes1 : vacinacoes) {
            if (!vacinacoes.isEmpty()) {
                System.out.println(vacinacoes1 + "\n");
            } else {
                System.out.println("Nenhuma vacina adicionada na ficha de vacinação!");
            }
        }

    }

    public static ArrayList<Vacinacao> addVacinacao() {
        if (!vacinas.isEmpty()) {
            System.out.println("Local da vacina: ");
            String local = sc.nextLine();
            mostraVacinas();
            System.out.print("digite o código da vacina: ");
            int codV = sc.nextInt();
            if (vacinas.containsKey(codV)) {

                vacinacoes.add(new Vacinacao(local, vacinas.get(codV)));
            } else {
                System.out.println("Vacina não cadastrada!");
            }
        } else {
            System.out.println("Nenhuma vacina cadastrada!");
        }
        return vacinacoes;

    }

    public static ArrayList<Endereco> addEndereco() {

        System.out.print("Digite o estado: ");
        String uf = sc.nextLine();
        System.out.print("Digite a cidade ");
        String cidade = sc.nextLine();
        System.out.print("Digite o bairro: ");
        String bairro = sc.nextLine();
        System.out.print("Digite a rua: ");
        String rua = sc.nextLine();
        System.out.print("Digite o número da residencia: ");
        int numResid = sc.nextInt();

        enderecos.add(new Endereco(uf, cidade, bairro, rua, numResid));

        return enderecos;

    }
    

}