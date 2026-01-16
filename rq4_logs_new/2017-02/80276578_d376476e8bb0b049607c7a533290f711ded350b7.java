package org.zkoss.reference.developer.spring.security.model;

import javax.persistence.*;
import java.util.Date;

@Entity
@Table(name = "muvement_history")
public class MuvementHistory {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    private Date date;
    private Integer placeId;
    private Integer oredrsId;
    private Integer ordersOrderUserId;
    private Integer ordersStatusHistoryId;
    private Integer ordersStatusHistoryStatusId;

    @ManyToOne
    @JoinColumn(name = "order_id", nullable = false)
    private Order order;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public Integer getPlaceId() {
        return placeId;
    }

    public void setPlaceId(Integer placeId) {
        this.placeId = placeId;
    }

    public Integer getOredrsId() {
        return oredrsId;
    }

    public void setOredrsId(Integer oredrsId) {
        this.oredrsId = oredrsId;
    }

    public Integer getOrdersOrderUserId() {
        return ordersOrderUserId;
    }

    public void setOrdersOrderUserId(Integer ordersOrderUserId) {
        this.ordersOrderUserId = ordersOrderUserId;
    }

    public Integer getOrdersStatusHistoryId() {
        return ordersStatusHistoryId;
    }

    public void setOrdersStatusHistoryId(Integer ordersStatusHistoryId) {
        this.ordersStatusHistoryId = ordersStatusHistoryId;
    }

    public Integer getOrdersStatusHistoryStatusId() {
        return ordersStatusHistoryStatusId;
    }

    public void setOrdersStatusHistoryStatusId(Integer ordersStatusHistoryStatusId) {
        this.ordersStatusHistoryStatusId = ordersStatusHistoryStatusId;
    }

    public Order getOrder() {
        return order;
    }

    public void setOrder(Order order) {
        this.order = order;
    }
}