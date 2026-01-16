package sysjm3.bulbo.bulbo.repositories;

import java.util.List;
import java.util.UUID;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.TypedQuery;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Component;
import sysjm3.bulbo.bulbo.model.Card;
import sysjm3.bulbo.bulbo.service.WorkspaceService;

/**
 *
 * @author Christoffer Hansen
 */
@Component
public class CardRepositoryCustomImpl implements CardRepositoryCustom {
    private final CardRepositoryBasic cardRepositoryBasic;

    @PersistenceContext
    private EntityManager entityManager;
    
    @Autowired
    private WorkspaceService workspaceService;
    
    // constructor-based injection
    public CardRepositoryCustomImpl(
        CardRepositoryBasic cardRepositoryBasic)
    {
        this.cardRepositoryBasic = cardRepositoryBasic;
    }

    @Override
    public List<Card> findByWorkspace(UUID id, Sort sort) {
        String queryStatement = "FROM Card WHERE workspace = :workspace";
        TypedQuery<Card> query = entityManager.createQuery(queryStatement, Card.class);
        query.setParameter("workspace", workspaceService.findOne(id));
        return query.getResultList();
    }
    
}