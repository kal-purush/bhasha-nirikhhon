package net.projecthade.jepret.web.rest.dto;

public class MessageErrorDTO {

    private String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public MessageErrorDTO() {
    }

    public MessageErrorDTO(String name) {
        this.name = name;
    }
}