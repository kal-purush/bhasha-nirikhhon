package ouacheter.backend.entities;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.OneToOne;

@Entity
public class Asset {
    @Id
    private Long id;
    private String coordonates;
    private String name;
    @OneToOne
    private User owner;

    public void setId(Long id) {
        this.id = id;
    }

    public String getCoordonates() {
        return coordonates;
    }

    public void setCoordonates(String coordonates) {
        this.coordonates = coordonates;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public User getOwner() {
        return owner;
    }

    public void setOwner(User owner) {
        this.owner = owner;
    }

    public Long getId() {
        return id;
    }
}