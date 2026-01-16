package index;

import util.TextoUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Indexador {
    ListaInvertida lista;

    public Indexador(ListaInvertida lista) {
        this.lista = lista;
    }

    public void indexarTitulo(int id, String titulo) throws Exception {
        List<String> palavras = TextoUtils.tokenizar(titulo);
        Map<String, Integer> tf = TextoUtils.calcularTF(palavras);

        for (Map.Entry<String, Integer> entrada : tf.entrySet()) {
            String termo = entrada.getKey();
            int freq = entrada.getValue();
            ElementoLista el = new ElementoLista(id, freq);
            lista.create(termo, el);
        }
    }

    public void removerDocumento(int id, String titulo) throws Exception {
        List<String> palavras = TextoUtils.tokenizar(titulo);
        Set<String> unicos = new HashSet<>(palavras);

        for (String termo : unicos) {
            lista.delete(termo, id);
        }
    }

    public void atualizarTitulo(int id, String tituloAntigo, String tituloNovo) throws Exception {
        removerDocumento(id, tituloAntigo);
        indexarTitulo(id, tituloNovo);
    }

    public void buscar(String termo) throws Exception {
        termo = TextoUtils.normalizar(termo);
        ElementoLista[] resultados = lista.read(termo);
        for (ElementoLista el : resultados) {
            System.out.println("ID: " + el.getId() + ", TF: " + el.getFrequencia());
        }
    }
}
