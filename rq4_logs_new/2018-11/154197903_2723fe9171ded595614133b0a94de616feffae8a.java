package bd;

import java.io.Serializable;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import model.Usuario;

/**
 *
 * @author valeria
 */

public class UsuarioBD implements Serializable{
    
    EntityManagerFactory emf = Persistence.createEntityManagerFactory("rscsis");
    EntityManager em = emf.createEntityManager();;
    
    public void create(Usuario usuario) {
        
        try {
            em.getTransaction().begin();
            em.persist(usuario);
            em.getTransaction().commit();
        } finally {
            if (em != null) {
                em.close();
            }
        }
    }
    
}