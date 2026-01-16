package modelo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CarrinhoCompras {
    private List<Item> itens = new ArrayList<>();

    public List<Item> getItens() {
        return Collections.unmodifiableList(itens);
    }

    public void adicionarItem(int quantidade, Produto produto) {
        itens.add(new Item(quantidade, produto));
    }
    
    public void removerItem(int posicaoItem) {
        itens.remove(posicaoItem);
    }
    
    public void aumentarQuantidade(int posicaoItem) {
        itens.get(posicaoItem).aumentarQuantidade();
    }
    
    public void diminuirQuantidade(int posicaoItem) {
        itens.get(posicaoItem).diminuirQuantidade();
    }
    
    public void esvaziar() {
        itens = new ArrayList<>();
    }
}