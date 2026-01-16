package com.rafaeldeluca.Ecommerce.Java.entities;

// classe de associação itens do pedido = acontece quando duas entidades tem uma relação many to many
// vai haver um terceira tabela no meio que tem itens além das duas chaves estrangeiras
// Tabela itemDeProduto vai ter o chave estrageira da tabela Order e da tabela Product e mais
// os itens adicionais

// as duas chaves estrangeiras juntas vão formar uma chave primária composta
// no java é necessário cliar um nova chave para representar a chave primária (primary key) pk

import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;

@Entity
@Table(name = "tb_order_item")
public class OrderItem {

    @EmbeddedId
    private OrderItemPK id = new OrderItemPK();
    private Integer quantidade;
    private Double preco;

    public OrderItem () {

    }
    // não expor a chave primária composta OrderItemPK no construtor
    //OrderItemPK já tem a referência para pedido e produto
    public OrderItem(Order pedido, Product produto, Integer quantidade, Double preco) {
        id.setProduto(produto);
        id.setPedido(pedido);
        this.quantidade = quantidade;
        this.preco = preco;
    }

    public Integer getQuantidade() {
        return quantidade;
    }

    public void setQuantidade(Integer quantidade) {
        this.quantidade = quantidade;
    }

    public Double getPreco() {
        return preco;
    }

    public void setPreco(Double preco) {
        this.preco = preco;
    }
    public Order getPedido () {
        return id.getPedido();
    }

    public void setPedido (Order pedido) {
        id.setPedido(pedido);
    }

    public Product getProduto () {
        return id.getProduto();
    }

    public void setProduto(Product produto) {
        id.setProduto(produto);
    }

}