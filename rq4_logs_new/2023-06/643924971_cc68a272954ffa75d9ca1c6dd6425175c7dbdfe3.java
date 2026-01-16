package com.example.ticketing.DTO;

public class TicketPersonDTO {
    private String name;
    private String id;
    private boolean isClient;

    public TicketPersonDTO(String name, String id, boolean isClient) {
        this.name = name;
        this.id = id;
        this.isClient = isClient;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public boolean isClient() {
        return isClient;
    }

    public void setClient(boolean client) {
        isClient = client;
    }

    @Override
    public String toString() {
        return "TicketPersonDTO{" +
                "name='" + name + '\'' +
                ", isClient=" + isClient +
                '}';
    }
}