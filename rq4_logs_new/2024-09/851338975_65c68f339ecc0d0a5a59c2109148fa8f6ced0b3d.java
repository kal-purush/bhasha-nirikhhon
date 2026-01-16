package com.reservibe.domain.entity.table;

import com.reservibe.domain.enums.table.TableStatus;
import com.reservibe.infra.model.table.TableModel;

import java.util.UUID;

public class Table {

    private UUID id;
    private Integer number;
    private int seats;
    private TableStatus status;

    public Table(UUID id, Integer number, int seats, TableStatus status) {
        this.id = id;
        this.number = number;
        this.seats = seats;
        this.status = status;
    }

    public Table(TableModel tableModel) {
        this(tableModel.getId(), tableModel.getNumber(), tableModel.getSeats(), tableModel.getStatus());
    }

    public UUID getId() {
        return id;
    }

    public int getNumber() {
        return number;
    }

    public int getSeats() {
        return seats;
    }

    public TableStatus getStatus() {
        return status;
    }

    public void setStatus(TableStatus status) {
        this.status = status;
    }
}