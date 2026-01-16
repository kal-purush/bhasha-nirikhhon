package ru.entity;

import javax.persistence.*;

@Entity
@Table(name = "devision")
public class DevisionEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "idDevision", length = 6, nullable = false, columnDefinition = "serial")
    private long idDevision;
    @Column(name = "namedevision", length = 150)
    private String namedevision ;
    @Column(name = "idcity", length = 8)
    private Integer idcity;

    public DevisionEntity() {} // Конструктор
    public long getIdDevision() {        return idDevision;    }
    public void setIdDevision(long idDevision) {        this.idDevision = idDevision;    }
    public String getNamedevision() {        return namedevision;    }
    public void setNamedevision(String namedevision) {        this.namedevision = namedevision;    }
    public Integer getIdcity() {        return idcity;    }
    public void setIdcity(Integer idcity) {        this.idcity = idcity;    }
}
