/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.po_crm_stub.models;

import com.mycompany.po_crm_stub.services.DuplicateResourceException;
import java.io.Serializable;
import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import javax.xml.bind.annotation.XmlRootElement;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.exception.ConstraintViolationException;

/**
 *
 * @author Tamashimaru
 */
@Entity
@Table(name = "attachment")
@XmlRootElement
@NamedQueries({
    @NamedQuery(name = "Attachment.findAll", query = "SELECT a FROM Attachment a")
    , @NamedQuery(name = "Attachment.findById", query = "SELECT a FROM Attachment a WHERE a.id = :id")
    , @NamedQuery(name = "Attachment.findByOrd", query = "SELECT a FROM Attachment a WHERE a.ord = :ord")
    , @NamedQuery(name = "Attachment.findByFileName", query = "SELECT a FROM Attachment a WHERE a.fileName = :fileName")})
public class Attachment implements Serializable {

    @Basic(optional = false)
    @NotNull
    @Lob
    @Column(name = "file_content")
    private byte[] fileContent;

    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "id")
    private Integer id;
    @Basic(optional = false)
    @NotNull
    @Column(name = "ord")
    private int ord;
    @Basic(optional = false)
    @NotNull
    @Size(min = 1, max = 255)
    @Column(name = "file_name")
    private String fileName;
    @JoinColumn(name = "email_id", referencedColumnName = "id")
    @ManyToOne(optional = false)
    private Email emailId;

    public Attachment() {
    }

    public Attachment(Integer id) {
        this.id = id;
    }
    
    public Attachment(Email emailId, int ord, String fileName, byte[] fileContent) {
        this.emailId = emailId;
        this.ord = ord;
        this.fileName = fileName;
        this.fileContent = fileContent;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public int getOrd() {
        return ord;
    }

    public void setOrd(int ord) {
        this.ord = ord;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public byte[] getFileContent() {
        return fileContent;
    }

    public void setFileContent(byte[] fileContent) {
        this.fileContent = fileContent;
    }

    public Email getEmailId() {
        return emailId;
    }

    public void setEmailId(Email emailId) {
        this.emailId = emailId;
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
        if (!(object instanceof Attachment)) {
            return false;
        }
        Attachment other = (Attachment) object;
        if ((this.id == null && other.id != null) || (this.id != null && !this.id.equals(other.id))) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "com.mycompany.po_crm_stub.models.Attachment[ id=" + id + " ]";
    }
    
    public static Integer insert(Email email, int ord, String fileName, String fileContent) throws DuplicateResourceException{
        
        Session session = HibernateUtil.createSessionFactory().openSession();
        Transaction tx = null;
        Attachment attachment = null;
        try {
            
            tx = session.beginTransaction();
            
            attachment = new Attachment(email, ord, fileName, fileContent.getBytes());
            session.persist(attachment);
           
            tx.commit();

        } catch (HibernateException e) {
            e.printStackTrace();
            if (tx != null) {
                tx.rollback();
            }
            if(e instanceof ConstraintViolationException){
                throw new DuplicateResourceException("Conflict");
            }
            else{
                return null;
            }
        } finally {
            session.close();
        }
        return attachment.getId();

    }
    
}