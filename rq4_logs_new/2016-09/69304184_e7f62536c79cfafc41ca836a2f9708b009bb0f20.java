/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.locadora.models;

import java.io.Serializable;
import java.util.Collection;
import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

/**
 *
 * @author guilherme
 */
@Entity
@Table(name = "Avaria")
@XmlRootElement
@NamedQueries({
    @NamedQuery(name = "Avaria.findAll", query = "SELECT a FROM Avaria a"),
    @NamedQuery(name = "Avaria.findByIdAvaria", query = "SELECT a FROM Avaria a WHERE a.idAvaria = :idAvaria"),
    @NamedQuery(name = "Avaria.findByDescricao", query = "SELECT a FROM Avaria a WHERE a.descricao = :descricao")})
public class Avaria implements Serializable {

    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "idAvaria")
    private Integer idAvaria;
    @Basic(optional = false)
    @Column(name = "descricao")
    private String descricao;
    @JoinTable(name = "Locacao_Avaria", joinColumns = {
        @JoinColumn(name = "idAvaria", referencedColumnName = "idAvaria")}, inverseJoinColumns = {
        @JoinColumn(name = "idLocacao", referencedColumnName = "idLocacao")})
    @ManyToMany
    private Collection<Locacao> locacaoCollection;

    public Avaria() {
    }

    public Avaria(Integer idAvaria) {
        this.idAvaria = idAvaria;
    }

    public Avaria(Integer idAvaria, String descricao) {
        this.idAvaria = idAvaria;
        this.descricao = descricao;
    }

    public Integer getIdAvaria() {
        return idAvaria;
    }

    public void setIdAvaria(Integer idAvaria) {
        this.idAvaria = idAvaria;
    }

    public String getDescricao() {
        return descricao;
    }

    public void setDescricao(String descricao) {
        this.descricao = descricao;
    }

    @XmlTransient
    public Collection<Locacao> getLocacaoCollection() {
        return locacaoCollection;
    }

    public void setLocacaoCollection(Collection<Locacao> locacaoCollection) {
        this.locacaoCollection = locacaoCollection;
    }

    @Override
    public int hashCode() {
        int hash = 0;
        hash += (idAvaria != null ? idAvaria.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object object) {
        // TODO: Warning - this method won't work in the case the id fields are not set
        if (!(object instanceof Avaria)) {
            return false;
        }
        Avaria other = (Avaria) object;
        if ((this.idAvaria == null && other.idAvaria != null) || (this.idAvaria != null && !this.idAvaria.equals(other.idAvaria))) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "models.Avaria[ idAvaria=" + idAvaria + " ]";
    }
    
}