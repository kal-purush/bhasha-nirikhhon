package doghost;

import java.util.Objects;

public class Dog {

    private String nome;
    private String tutor;
    private int consumoRacao;

    public Dog(String nome, String tutor, int consumoRacao) {
        this(nome, tutor, consumoRacao, false);
    }

    public Dog(String nome, String tutor, int consumoRacao, boolean restricao) {
        this.nome = nome;
        this.tutor = tutor;
        this.consumoRacao = consumoRacao;
        if (restricao) {
            this.consumoRacao *= 2;
        }
    }

    public int getConsumoRacao() {
        return consumoRacao;
    }

    @Override
    public String toString() {
        return "Dog [name=" + nome + ", tutor=" + tutor + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Dog dog = (Dog) o;
        return nome.equals(dog.nome) && tutor.equals(dog.tutor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nome, tutor);
    }
}