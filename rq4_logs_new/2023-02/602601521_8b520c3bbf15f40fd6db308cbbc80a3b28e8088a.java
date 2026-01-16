package com.rafaeldeluca.Ecommerce.Java.entities;

import jakarta.persistence.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Entity
@Table (name = "tb_category")
public class Category {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String nome;

    // Como a relação é muitos para muitos, para não repetir o Id na Tabela de relacionamento gerada
    // deve-se usar o tipo Set e não List. Colletion set não aceita valores repetidos do par key, value
    @ManyToMany(mappedBy = "categorias")
    private Set<Product> produtos = new HashSet<Product>();

    public Category () {

    }

    public Category(Long id, String nome) {
        this.id = id;
        this.nome = nome;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getNome() {
        return nome;
    }

    public void setNome(String nome) {
        this.nome = nome;
    }

    public Set<Product> getProdutos() {
        return produtos;
    }

    /*
    public void setProdutos(Set<Product> produtos) {
        this.produtos = produtos;
    }

     */
}