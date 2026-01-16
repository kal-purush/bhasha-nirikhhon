package com.rafaeldeluca.Ecommerce.Java.entities;

import com.rafaeldeluca.Ecommerce.Java.entities.enums.OrderStatus;
import jakarta.persistence.*;

import java.time.Instant;

@Entity
@Table(name="tb_order")
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(columnDefinition ="TIMESTAMP WITHOUT TIME ZONE") // salvar no bando com padrão UTC
    private Instant momento;
    private OrderStatus statusDoPedido;

    @ManyToOne
    @JoinColumn(name = "cliente_id") // chave estrangeira no banco
    private User cliente;

    public Order () {

    }

    public Order(Long id, Instant momento, OrderStatus statusDoPedido, User cliente) {
        this.id = id;
        this.momento = momento;
        this.statusDoPedido = statusDoPedido;
        this.cliente = cliente;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Instant getMomento() {
        return momento;
    }

    public void setMomento(Instant momento) {
        this.momento = momento;
    }

    public OrderStatus getStatusDoPedido() {
        return statusDoPedido;
    }

    public void setStatusDoPedido(OrderStatus statusDoPedido) {
        this.statusDoPedido = statusDoPedido;
    }

    public User getCliente() {
        return cliente;
    }

    public void setCliente(User cliente) {
        this.cliente = cliente;
    }
}