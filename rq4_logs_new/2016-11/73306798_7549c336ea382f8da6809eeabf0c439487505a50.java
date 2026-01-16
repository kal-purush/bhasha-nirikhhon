/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hansen.koeln.database.finanzen;

import java.io.Serializable;
import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.xml.bind.annotation.XmlRootElement;

/**
 *
 * @author uli
 */
@Entity
@Table(name = "ausgaben_kategorien")
@XmlRootElement
@NamedQueries({
    @NamedQuery(name = "AusgabenKategorien_1.findAll", query = "SELECT a FROM AusgabenKategorien_1 a"),
    @NamedQuery(name = "AusgabenKategorien_1.findById", query = "SELECT a FROM AusgabenKategorien_1 a WHERE a.id = :id"),
    @NamedQuery(name = "AusgabenKategorien_1.findByName", query = "SELECT a FROM AusgabenKategorien_1 a WHERE a.name = :name"),
    @NamedQuery(name = "AusgabenKategorien_1.findByParentID", query = "SELECT a FROM AusgabenKategorien_1 a WHERE a.parentID = :parentID")})
public class AusgabenKategorien implements Serializable {

    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "ID")
    private Integer id;
    @Basic(optional = false)
    @Column(name = "Name")
    private String name;
    @Basic(optional = false)
    @Column(name = "ParentID")
    private int parentID;

    public AusgabenKategorien() {
    }

    public AusgabenKategorien(Integer id) {
        this.id = id;
    }

    public AusgabenKategorien(Integer id, String name, int parentID) {
        this.id = id;
        this.name = name;
        this.parentID = parentID;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getParentID() {
        return parentID;
    }

    public void setParentID(int parentID) {
        this.parentID = parentID;
    }

    @Override
    public int hashCode() {
        int hash = 0;
        hash += (id != null ? id.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object object) {
        // TODO: Warning - this method won't work in the case the id fields are not set
        if (!(object instanceof AusgabenKategorien)) {
            return false;
        }
        AusgabenKategorien other = (AusgabenKategorien) object;
        if ((this.id == null && other.id != null) || (this.id != null && !this.id.equals(other.id))) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "hansen.koeln.database.finanzen.AusgabenKategorien_1[ id=" + id + " ]";
    }
    
}