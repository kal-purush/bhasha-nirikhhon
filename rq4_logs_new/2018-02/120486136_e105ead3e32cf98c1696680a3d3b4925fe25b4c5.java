/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Entite;

import java.util.logging.Logger;

/**
 *
 * @author khaled22
 */
public class Articles {
    private int id ;          private String titre ;
    private String sub_t ;    private String descript ;
    private String image ;    private int id_user ;

    public Articles(String titre, String sub_t, String descript, String image, int id_user) {
        this.titre = titre;
        this.sub_t = sub_t;
        this.descript = descript;
        this.image = image;
        this.id_user = id_user;
    }

    public Articles(int id) {
        this.id = id;
    }
    private static final Logger LOG = Logger.getLogger(Articles.class.getName());

    public int getId() {
        return id;
    }                   public void setId(int id) {
        this.id = id;
    }

    public String getTitre() {
        return titre;
    }             public void setTitre(String titre) {
        this.titre = titre;
    }

    public String getSub_t() {
        return sub_t;
    }             public void setSub_t(String sub_t) {
        this.sub_t = sub_t;
    }

    public String getDescript() {
        return descript;
    }          public void setDescript(String descript) {
        this.descript = descript;
    }

    public String getImage() {
        return image;
    }             public void setImage(String image) {
        this.image = image;
    }

    public int getId_user() {
        return id_user;
    }              public void setId_user(int id_user) {
        this.id_user = id_user;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 97 * hash + this.id;
        hash = 97 * hash + this.id_user;
        return hash;
    }                   @Override 
                                                       public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Articles other = (Articles) obj;
        if (this.id != other.id) {
            return false;
        }
        if (this.id_user != other.id_user) {
            return false;
        }
        return true;
    }

                                       @Override
                             public String toString() {
        return "Articles{" + "id=" + id + ", titre=" + titre + ", sub_t=" + sub_t + ", descript=" + descript + ", image=" + image + ", id_user=" + id_user + '}';
    }
    
    
    
    
    
}