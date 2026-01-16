package br.com.estudo.storechallenge.challenge.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Builder;
import lombok.Data;

import javax.persistence.*;
import java.util.Date;
import java.util.List;

@Data
@Entity
@Builder
@Table(name = "\"order\"")
public class Order {

    @Id
    private Long id;

    @ManyToOne
    @JsonIgnore
    @JoinColumn(name = "store_id", nullable = false)
    private Store store;

    @Column(name = "\"user\"")
    private String user;

    private String address;

    @Temporal(TemporalType.DATE)
    @Column(name = "confirmation_date")
    private Date confirmationDate;

    // TODO: criar enum StatusOrder
    private Integer status;

    @OneToMany(mappedBy = "order")
    private List<OrderItem> items;

    @OneToOne
    @JoinColumn(name = "payment_id", nullable=true)
    private Payment payment;

    @Temporal(TemporalType.DATE)
    @Column(name = "creation_date")
    private Date creationDate;

    @Temporal(TemporalType.DATE)
    @Column(name = "update_date")
    private Date updateDate;

}