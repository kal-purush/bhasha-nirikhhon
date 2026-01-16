package persistence;

import model.Apartment;
import util.DBUtil;

import javax.persistence.EntityManager;

public class RepositoryApartment {
    private EntityManager em;

    public RepositoryApartment() {
        this.em = DBUtil.getEntityManager();
    }

    public void saveApartment(Apartment apartment) {
        try{
            this.em.getTransaction().begin();
            this.em.persist(apartment);
            this.em.getTransaction().commit();
        }catch (Exception e) {
            this.em.getTransaction().rollback();
        }
    }

    public void updateApartment(Apartment apartment) {
        try{
            this.em.getTransaction().begin();
            this.em.merge(apartment);
            this.em.getTransaction().commit();
        }catch (Exception e) {
            this.em.getTransaction().rollback();
        }
    }

    public void deleteApartment(Apartment apartment) {
        try {
            this.em.getTransaction().begin();
            this.em.remove(em.merge(apartment));
            this.em.getTransaction().commit();
        } catch (Exception e) {
            this.em.getTransaction().rollback();
        }
    }
}