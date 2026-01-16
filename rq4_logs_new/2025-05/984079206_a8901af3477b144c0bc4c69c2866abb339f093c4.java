// GerenciadorBiblioteca.java
package ifsc.poo.biblioteca;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class GerenciadorBiblioteca {
    private List<Autor> autores = new ArrayList<>();
    private List<Livro> livros = new ArrayList<>();
    private List<Leitor> leitores = new ArrayList<>();
    private List<Emprestimo> emprestimos = new ArrayList<>();

    public void cadastrarAutor(Autor autor) {
        autores.add(autor);
    }

    public String listarAutores() {
        Collections.sort(autores, Comparator.comparing(Autor::getNome));
        return autores.stream()
                .map(Autor::dadosAutor)
                .collect(Collectors.joining("\n"));
    }

    public void cadastrarLivro(Livro livro) {
        livros.add(livro);
    }

    public void associarLivroAutor(Livro livro, Autor autor) {
        if (!livros.contains(livro)) {
            cadastrarLivro(livro);
        }
        AutorLivro autorLivro = new AutorLivro();
        autorLivro.setAutor(autor);
        autorLivro.setLivro(livro);
        autor.addLivros(autorLivro);
        livro.setAutorLivro(autorLivro);
    }

    public String listarLivros() {
        StringBuilder builder = new StringBuilder();

        builder.append("Por título:\n");
        livros.stream()
                .sorted(Comparator.comparing(Livro::getTitulo))
                .forEach(l -> builder.append(l.dadosLivro()).append("\n"));

        builder.append("\nPor autor:\n");
        livros.stream()
                .sorted(Comparator.comparing(l -> l.getAutorLivro().getAutor().getNome()))
                .forEach(l -> builder.append(l.dadosLivro()).append("\n"));

        builder.append("\nPor ISBN:\n");
        livros.stream()
                .sorted(Comparator.comparing(Livro::getIsbn))
                .forEach(l -> builder.append(l.dadosLivro()).append("\n"));

        return builder.toString();
    }

    public Leitor cadastrarLeitor(String nome, String endereco, String telefone) {
        Leitor leitor = new Leitor();
        leitor.setNome(nome);
        leitor.setEndereco(endereco);
        leitor.setTelefone(telefone);
        leitor.setId(leitores.size());
        leitores.add(leitor);
        return leitor;
    }

    public String listarLeitores() {
        StringBuilder builder = new StringBuilder();

        builder.append("Por nome:\n");
        leitores.stream()
                .sorted(Comparator.comparing(Leitor::getNome))
                .forEach(l -> builder.append(l.dadosLeitor()).append("\n"));

        builder.append("\nPor ID:\n");
        leitores.stream()
                .sorted(Comparator.comparing(Leitor::getId))
                .forEach(l -> builder.append(l.dadosLeitor()).append("\n"));

        return builder.toString();
    }

    public boolean registrarEmprestimo(Leitor leitor, Livro livro, LocalDate data) {
        if (livro.getCopias() <= 0) {
            System.out.println("Não há cópias disponíveis deste livro.");
            return false;
        }

        if (leitor.getEmprestimos().stream()
                .anyMatch(e -> e.getLivro().equals(livro))) {
            System.out.println("O leitor já possui este livro emprestado.");
            return false;
        }

        if (leitor.getEmprestimos().size() >= 5) {
            System.out.println("O leitor já possui 5 livros emprestados.");
            return false;
        }

        Emprestimo emprestimo = new Emprestimo();
        emprestimo.setDataEmprestimo(data);
        emprestimo.setLivro(livro);
        emprestimo.setLeitor(leitor);

        leitor.emprestimo(emprestimo);
        livro.decrementCopias();
        emprestimos.add(emprestimo);

        return true;
    }

    //Listar os empréstimos
    public String listarEmprestimos() {
        Collections.sort(emprestimos, Comparator.comparing(Emprestimo::getDataEmprestimo));
        return emprestimos.stream()
                .map(Emprestimo::dadosEmprestimo)
                .collect(Collectors.joining("\n"));
    }

    public String listarEmprestimosPorLeitor(Leitor leitor) {
        return leitor.getEmprestimos().stream()
                .sorted(Comparator.comparing(Emprestimo::getDataEmprestimo))
                .map(Emprestimo::dadosEmprestimo)
                .collect(Collectors.joining("\n"));
    }
}