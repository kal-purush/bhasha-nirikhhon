package com.rafaeldeluca.Ecommerce.Java.entities;

import jakarta.persistence.*;

import java.time.Instant;

@Entity
@Table(name = "tb_payment")
public class Payment {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(columnDefinition ="TIMESTAMP WITHOUT TIME ZONE")
    private Instant momentoDoPagamento;

    @OneToOne
    @MapsId
    private Order pedido;

    public Payment () {

    }

    public Payment(Long id, Instant momentoDoPagamento, Order pedido) {
        this.id = id;
        this.momentoDoPagamento = momentoDoPagamento;
        this.pedido = pedido;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Instant getMomentoDoPagamento() {
        return momentoDoPagamento;
    }

    public void setMomentoDoPagamento(Instant momentoDoPagamento) {
        this.momentoDoPagamento = momentoDoPagamento;
    }

    public Order getPedido() {
        return pedido;
    }

    public void setPedido(Order pedido) {
        this.pedido = pedido;
    }
}