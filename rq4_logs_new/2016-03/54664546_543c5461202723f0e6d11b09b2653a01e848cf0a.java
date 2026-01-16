/*
 * Created by Eric Leszcynski on 2016.03.28  * 
 * Copyright © 2016 Eric Leszcynski. All rights reserved. * 
 */
package com.ryde.entity;

import java.io.Serializable;
import java.util.Collection;
import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

/**
 *
 * @author Eric
 */
@Entity
@Table(name = "Group_Table")
@XmlRootElement
@NamedQueries({
    @NamedQuery(name = "GroupTable.findAll", query = "SELECT g FROM GroupTable g"),
    @NamedQuery(name = "GroupTable.findById", query = "SELECT g FROM GroupTable g WHERE g.id = :id"),
    @NamedQuery(name = "GroupTable.findByTitle", query = "SELECT g FROM GroupTable g WHERE g.title = :title"),
    @NamedQuery(name = "GroupTable.findByDescription", query = "SELECT g FROM GroupTable g WHERE g.description = :description")})
public class GroupTable implements Serializable {

    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "id")
    private Integer id;
    @Basic(optional = false)
    @NotNull
    @Size(min = 1, max = 64)
    @Column(name = "title")
    private String title;
    @Basic(optional = false)
    @NotNull
    @Size(min = 1, max = 64)
    @Column(name = "description")
    private String description;
    @OneToMany(cascade = CascadeType.ALL, mappedBy = "groupId")
    private Collection<GroupUser> groupUserCollection;
    @OneToMany(cascade = CascadeType.ALL, mappedBy = "groupId")
    private Collection<GroupTimeslot> groupTimeslotCollection;

    public GroupTable() {
    }

    public GroupTable(Integer id) {
        this.id = id;
    }

    public GroupTable(Integer id, String title, String description) {
        this.id = id;
        this.title = title;
        this.description = description;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @XmlTransient
    public Collection<GroupUser> getGroupUserCollection() {
        return groupUserCollection;
    }

    public void setGroupUserCollection(Collection<GroupUser> groupUserCollection) {
        this.groupUserCollection = groupUserCollection;
    }

    @XmlTransient
    public Collection<GroupTimeslot> getGroupTimeslotCollection() {
        return groupTimeslotCollection;
    }

    public void setGroupTimeslotCollection(Collection<GroupTimeslot> groupTimeslotCollection) {
        this.groupTimeslotCollection = groupTimeslotCollection;
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
        if (!(object instanceof GroupTable)) {
            return false;
        }
        GroupTable other = (GroupTable) object;
        if ((this.id == null && other.id != null) || (this.id != null && !this.id.equals(other.id))) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "com.ryde.entity.GroupTable[ id=" + id + " ]";
    }
    
}