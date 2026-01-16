package aula7.sistema;

public class Cliente {
    private String nome;
    private String cpf;
    private String endereco;

    public Cliente() {
    }

    public Cliente(String novoNome, String novoCpf, String novoEndereco) {
        nome = novoNome;
        cpf = novoCpf;
        endereco = novoEndereco;
    }

    public String getNome() {
        return nome;
    }

    public void setNome(String novoNome) {
        nome = novoNome;
    }

    public String getCpf() {
        return cpf;
    }

    public void setCpf(String novoCpf) {
        cpf = novoCpf;
    }

    public String getEndereco() {
        return endereco;
    }

    public void setEndereco(String novoEndereco) {
        endereco = novoEndereco;
    }
}