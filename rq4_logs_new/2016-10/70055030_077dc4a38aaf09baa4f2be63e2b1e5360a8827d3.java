package pamm.dal;

import play.Logger;
import play.db.jpa.JPA;

import javax.persistence.EntityManager;

public class EntityManagerProvider
{
    private final Logger.ALogger logger = Logger.of(this.getClass());

    public EntityManager getEntityManager()
    {
        EntityManager em = null;
        try
        {
            logger.info("Getting Entity manager ");
            em = JPA.em();
        }
        catch (Throwable t)
        {
            logger.info("Error creating Entity manager " + t.getMessage());
        }
        logger.info("Returning Entity Manager " + em);
        return em;
    }
}