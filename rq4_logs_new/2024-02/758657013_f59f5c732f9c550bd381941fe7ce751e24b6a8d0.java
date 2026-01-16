package estrutura.listaEncadeada;

public class ListaEncadeada<T> {

    private NoLista<T> primeiro;

    private int comprimento;

    public ListaEncadeada() {
        primeiro = null;
        comprimento = 0;
    }

    public NoLista<T> getPrimeiro() {
        return primeiro;
    }

    public void inserir(T info) {

        NoLista<T> novoNo = new NoLista<T>();
        novoNo.setProximo(primeiro);
        novoNo.setInfo(info);
        primeiro = novoNo;
        comprimento++;
    }

    public boolean estaVazia() {
        if (primeiro == null) {
            return true;
        }
        return primeiro.equals(null);
    }

    public NoLista<T> buscar(T info) {
        NoLista<T> atual = getPrimeiro();
        while (atual != null) {
            if (atual.getInfo().equals(info)) {
                return atual;
            }
            atual = atual.getProximo();
        }
        return null;

    }

    public void retirar(T info) {
        if (primeiro == null) {
            return;
        }

        if (primeiro.getInfo().equals(info)) {
            primeiro = primeiro.getProximo();
            return;
        }

        NoLista<T> anterior = primeiro;
        NoLista<T> atual = primeiro.getProximo();
        comprimento--;

        while (atual != null) {
            if (atual.getInfo().equals(info)) {
                anterior.setProximo(atual.getProximo());
                return;
            }
            anterior = atual;
            atual = atual.getProximo();
        }
    }

    public int getComprimento() {
        return comprimento;
    }

    public NoLista<T> obterNode(int index) {
        if (index < 0 || index >= getComprimento()) {
            throw new IndexOutOfBoundsException("Índice fora dos limites da lista");
        }

        NoLista<T> atual = primeiro;
        while ((atual != null) && (index > 0)) {
            index--;
            atual = atual.getProximo();
        }

        if (atual == null) {
            throw new IndexOutOfBoundsException();
        }

        return atual;

    }

    public String toString() {
        if (primeiro == null) {
            return "";
        }

        String result = "";
        NoLista<T> atual = primeiro;

        while (atual != null) {
            result += atual.getInfo();
            if (atual.getProximo() != null) {
                result += ", ";
            }
            atual = atual.getProximo();
        }

        return result;
    }

    public void imprimir() {
        NoLista<T> atual = primeiro;
        int posicao = 0;
        while (atual != null) {

            if (atual.getProximo() != null) {
                System.out.println(posicao + " - " + atual.getInfo() + " | " + atual.getProximo().getInfo());
                posicao++;
            } else {
                System.out.println(posicao + " - " + atual.getInfo() + " | null");

            }
            atual = atual.getProximo();
        }
    }
}