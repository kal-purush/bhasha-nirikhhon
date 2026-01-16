package com.example.rseservice.entity;

import javax.persistence.*;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@Entity
public class Script {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    @NotBlank(message = "scripts-1")
    private String content;

    @NotNull(message = "scripts-2")
    private Long howManyArguments;

    @ManyToOne
    private Client client;

    public Script() {
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public Long getHowManyArguments() {
        return howManyArguments;
    }

    public void setHowManyArguments(Long howManyArguments) {
        this.howManyArguments = howManyArguments;
    }

    public Client getClient() {
        return client;
    }

    public void setClient(Client client) {
        this.client = client;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final Script script = (Script) o;

        if (id != null ? !id.equals(script.id) : script.id != null) return false;
        if (content != null ? !content.equals(script.content) : script.content != null) return false;
        if (howManyArguments != null ? !howManyArguments.equals(script.howManyArguments) : script.howManyArguments != null)
            return false;
        return client != null ? client.equals(script.client) : script.client == null;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (content != null ? content.hashCode() : 0);
        result = 31 * result + (howManyArguments != null ? howManyArguments.hashCode() : 0);
        result = 31 * result + (client != null ? client.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Script{" +
                "id=" + id +
                ", content='" + content + '\'' +
                ", howManyArguments=" + howManyArguments +
                ", client=" + client +
                '}';
    }


    public static final class Build {
        private Long id;
        private String content;
        private Long howManyArguments;
        private Client client;

        private Build() {
        }

        public static Build aScript() {
            return new Build();
        }

        public Build id(Long id) {
            this.id = id;
            return this;
        }

        public Build content(String content) {
            this.content = content;
            return this;
        }

        public Build howManyArguments(Long howManyArguments) {
            this.howManyArguments = howManyArguments;
            return this;
        }

        public Build client(Client client) {
            this.client = client;
            return this;
        }

        public Script build() {
            Script script = new Script();
            script.setId(id);
            script.setContent(content);
            script.setHowManyArguments(howManyArguments);
            script.setClient(client);
            return script;
        }
    }
}