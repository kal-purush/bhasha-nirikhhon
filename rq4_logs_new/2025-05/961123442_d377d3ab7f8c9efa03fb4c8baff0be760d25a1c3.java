package controle;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Scanner;
import modelo.Ator;
import util.*;

public class ControleAtores {
    private Arquivo<Ator> arqAtor;
    private ArvoreBMais<AtorSerie> arvoreAtorSerie;
    private ArvoreBMais<SerieAtor> arvoreSerieAtor;
    private Scanner scanner;

    public ControleAtores() throws Exception {
        arqAtor = new Arquivo<>("ator", Ator.class.getConstructor());
        arvoreAtorSerie = new ArvoreBMais<>(AtorSerie.class.getConstructor(), 4, "ator_serie.db");
        arvoreSerieAtor = new ArvoreBMais<>(SerieAtor.class.getConstructor(), 4, "serie_ator.db");
        scanner = new Scanner(System.in);
    }

    public void menu() throws Exception {
        int opcao;
        do {
            System.out.println("\nMENU ATORES");
            System.out.println("1) Cadastrar novo ator");
            System.out.println("2) Buscar ator por ID");
            System.out.println("3) Listar todos os atores");
            System.out.println("4) Atualizar ator");
            System.out.println("5) Excluir ator");
            System.out.println("6) Listar séries do ator");
            System.out.println("0) Voltar");
            System.out.print("\nOpção: ");

            opcao = scanner.nextInt();
            scanner.nextLine(); // Limpar buffer

            switch (opcao) {
                case 1:
                    cadastrarAtor();
                    break;
                case 2:
                    buscarAtor();
                    break;
                case 3:
                    listarAtores();
                    break;
                case 4:
                    atualizarAtor();
                    break;
                case 5:
                    excluirAtor();
                    break;
                case 6:
                    listarSeriesDoAtor();
                    break;
                case 0:
                    break;
                default:
                    System.out.println("Opção inválida!");
            }
        } while (opcao != 0);
    }

    // Métodos com validações
    public void cadastrarAtor() throws Exception {
        System.out.println("\nCADASTRAR ATOR");

        System.out.print("ID do ator: ");
        int id = scanner.nextInt();
        scanner.nextLine();

        // Verifica se ID já existe
        if (arqAtor.read(id) != null) {
            System.out.println("Erro: ID já está em uso!");
            return;
        }

        System.out.print("Nome do ator: ");
        String nome = scanner.nextLine();

        // Valida nome não vazio
        if (nome.trim().isEmpty()) {
            System.out.println("Erro: Nome não pode ser vazio!");
            return;
        }

        Ator novoAtor = new Ator(id, nome);
        criar(novoAtor);
        System.out.println("Ator cadastrado com sucesso!");
    }

    public void buscarAtor() throws Exception {
        System.out.println("\nBUSCAR ATOR");
        System.out.print("Digite o ID do ator: ");
        int id = scanner.nextInt();
        scanner.nextLine();

        Ator ator = ler(id);
        if (ator == null) {
            System.out.println("Ator não encontrado!");
            return;
        }

        System.out.println("\nDADOS DO ATOR:");
        System.out.println("ID: " + ator.getID());
        System.out.println("Nome: " + ator.getNome());

        // Mostra séries vinculadas
        System.out.println("\nSÉRIES PARTICIPADAS:");
        ArrayList<AtorSerie> series = arvoreAtorSerie.read(new AtorSerie(id, 0));
        if (series.isEmpty()) {
            System.out.println("Nenhuma série vinculada");
        } else {
            for (AtorSerie as : series) {
                System.out.println("- Série ID: " + as.getIdSerie());
            }
        }
    }

    public void listarAtores() throws Exception {
        System.out.println("\nLISTA DE ATORES:");
        // Implemente conforme sua estrutura de arquivo
        // Exemplo genérico:
        for (int i = 1; i <= arqAtor.getUltimoID(); i++) {
            Ator a = arqAtor.read(i);
            if (a != null) {
                System.out.println("ID: " + a.getID() + " | Nome: " + a.getNome());
            }
        }
    }

    public void atualizarAtor() throws Exception {
        System.out.println("\nATUALIZAR ATOR");
        System.out.print("Digite o ID do ator: ");
        int id = scanner.nextInt();
        scanner.nextLine();

        Ator ator = ler(id);
        if (ator == null) {
            System.out.println("Ator não encontrado!");
            return;
        }

        System.out.println("Dados atuais:");
        System.out.println("Nome: " + ator.getNome());

        System.out.print("\nNovo nome (deixe em branco para manter): ");
        String novoNome = scanner.nextLine();

        if (!novoNome.trim().isEmpty()) {
            Ator atorAtualizado = new Ator(id, novoNome);
            if (arqAtor.excluir(id) && criar(atorAtualizado) == id) {
                System.out.println("Ator atualizado com sucesso!");
            } else {
                System.out.println("Falha ao atualizar ator!");
            }
        } else {
            System.out.println("Nenhuma alteração realizada.");
        }
    }

    public void excluirAtor() throws Exception {
        System.out.println("\nEXCLUIR ATOR");
        System.out.print("Digite o ID do ator: ");
        int id = scanner.nextInt();
        scanner.nextLine();

        // Verifica se o ator existe
        Ator ator = ler(id);
        if (ator == null) {
            System.out.println("Ator não encontrado!");
            return;
        }

        // Verifica se há vínculos com séries
        ArrayList<AtorSerie> vinculos = arvoreAtorSerie.read(new AtorSerie(id, 0));
        if (!vinculos.isEmpty()) {
            System.out.println("Erro: Ator não pode ser excluído pois está vinculado a " +
                    vinculos.size() + " série(s)!");
            System.out.println("Remova os vínculos antes de excluir o ator.");
            return;
        }

        if (deletar(id)) {
            System.out.println("Ator excluído com sucesso!");
        } else {
            System.out.println("Falha ao excluir ator!");
        }
    }

    private void listarSeriesDoAtor() throws Exception {
        System.out.println("\nSÉRIES DO ATOR");
        System.out.print("Digite o nome do ator (ou parte do nome): ");
        String nomeBusca = scanner.nextLine().toLowerCase();

        // Lista todos os atores cujo nome contém o texto buscado
        ArrayList<Ator> atoresEncontrados = new ArrayList<>();
        for (int i = 1; i <= arqAtor.getUltimoID(); i++) {
            Ator a = arqAtor.read(i);
            if (a != null && a.getNome().toLowerCase().contains(nomeBusca)) {
                atoresEncontrados.add(a);
            }
        }

        if (atoresEncontrados.isEmpty()) {
            System.out.println("Nenhum ator encontrado com esse nome!");
            return;
        }

        // Se encontrou apenas um, mostra diretamente
        if (atoresEncontrados.size() == 1) {
            Ator ator = atoresEncontrados.get(0);
            System.out.println("\nSéries vinculadas ao ator: " + ator.getNome());
            listarSeriesPorAtor(ator.getID());
            return;
        }

        // Se encontrou vários, mostra lista para seleção
        System.out.println("\nForam encontrados " + atoresEncontrados.size() + " atores:");
        for (int i = 0; i < atoresEncontrados.size(); i++) {
            System.out.println((i+1) + ") " + atoresEncontrados.get(i).getNome());
        }

        System.out.print("\nSelecione o número do ator desejado: ");
        int selecao = scanner.nextInt();
        scanner.nextLine();

        if (selecao < 1 || selecao > atoresEncontrados.size()) {
            System.out.println("Seleção inválida!");
            return;
        }

        Ator atorSelecionado = atoresEncontrados.get(selecao-1);
        System.out.println("\nSéries vinculadas ao ator: " + atorSelecionado.getNome());
        listarSeriesPorAtor(atorSelecionado.getID());
    }

    public int criar(Ator ator) throws Exception {
        return arqAtor.incluir(ator);
    }

    public Ator ler(int id) throws Exception {
        return arqAtor.read(id);
    }

    public boolean atualizarVinculosAtor(int idAtor, int[] novosIdsSeries) throws Exception {
        // 1. Primeiro, remove todos os vínculos existentes do ator
        ArrayList<AtorSerie> vinculosAntigos = arvoreAtorSerie.read(new AtorSerie(idAtor, 0));

        for (AtorSerie vinculo : vinculosAntigos) {
            int idSerie = vinculo.getIdSerie();
            arvoreSerieAtor.delete(new SerieAtor(idSerie, idAtor));
            arvoreAtorSerie.delete(vinculo);
        }

        // 2. Agora cria os novos vínculos
        for (int idSerie : novosIdsSeries) {
            arvoreAtorSerie.create(new AtorSerie(idAtor, idSerie));
            arvoreSerieAtor.create(new SerieAtor(idSerie, idAtor));
        }

        return true;
    }

    public boolean atualizarVinculosSerie(int idSerie, int[] novosIdsAtores) throws Exception {
        // 1. Primeiro, remove todos os vínculos existentes da série
        ArrayList<SerieAtor> vinculosAntigos = arvoreSerieAtor.read(new SerieAtor(idSerie, 0));

        for (SerieAtor vinculo : vinculosAntigos) {
            int idAtor = vinculo.getIdAtor();
            arvoreAtorSerie.delete(new AtorSerie(idAtor, idSerie));
            arvoreSerieAtor.delete(vinculo);
        }

        // 2. Agora cria os novos vínculos
        for (int idAtor : novosIdsAtores) {
            arvoreSerieAtor.create(new SerieAtor(idSerie, idAtor));
            arvoreAtorSerie.create(new AtorSerie(idAtor, idSerie));
        }

        return true;
    }

    public boolean deletar(int id) throws Exception {
        // Remove os vínculos antes de deletar o ator
        ArrayList<AtorSerie> vinculos = arvoreAtorSerie.read(new AtorSerie(id, 0));

        for (AtorSerie vinculo : vinculos) {
            int idSerie = vinculo.getIdSerie();
            arvoreSerieAtor.delete(new SerieAtor(idSerie, id));
        }

        // Remove todos os vínculos do ator
        for (AtorSerie vinculo : vinculos) {
            arvoreAtorSerie.delete(vinculo);
        }

        return arqAtor.excluir(id);
    }

    public void vincularAtorSerie(int idAtor, int idSerie) throws Exception {
        arvoreAtorSerie.create(new AtorSerie(idAtor, idSerie));
        arvoreSerieAtor.create(new SerieAtor(idSerie, idAtor));
    }

    public void deletarVinculo(int idAtor, int idSerie) throws Exception {
        arvoreAtorSerie.delete(new AtorSerie(idAtor, idSerie));
        arvoreSerieAtor.delete(new SerieAtor(idSerie, idAtor));
    }

    public void listarSeriesPorAtor(int idAtor) throws Exception {
        ArrayList<AtorSerie> vinculos = arvoreAtorSerie.read(new AtorSerie(idAtor, 0));

        for (AtorSerie vinculo : vinculos) {
            System.out.println("ID da série vinculada: " + vinculo.getIdSerie());
        }
    }

    public void listarAtoresPorSerie(int idSerie) throws Exception {
        ArrayList<SerieAtor> vinculos = arvoreSerieAtor.read(new SerieAtor(idSerie, 0));

        for (SerieAtor vinculo : vinculos) {
            System.out.println("ID do ator vinculado: " + vinculo.getIdAtor());
        }
    }
}