package com.yrgo.dataaccess;

import com.yrgo.domain.Action;
import org.springframework.stereotype.Repository;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.transaction.Transactional;
import java.util.List;

@Repository
@Transactional
public class ActionDaoJpaImpl implements ActionDao {

    private static final String GET_INCOMPLETE_ACTIONS_SQL = "SELECT a FROM Action a WHERE a.owningUser = :userId AND" +
        " a.complete = false";

    @PersistenceContext
    private EntityManager em;

    @Override
    public void create(Action newAction) {
        em.persist(newAction);
    }

    @Override
    public List<Action> getIncompleteActions(String userId) {
        return em
            .createQuery(GET_INCOMPLETE_ACTIONS_SQL, Action.class)
            .setParameter("userId", userId)
            .getResultList();
    }

    @Override
    public void update(Action actionToUpdate) throws RecordNotFoundException {
        Action currentAction = em.find(Action.class, actionToUpdate.getActionId());
        if (currentAction == null) {
            throw new RecordNotFoundException();
        }
        currentAction.setDetails(actionToUpdate.getDetails());
        currentAction.setRequiredBy(actionToUpdate.getRequiredBy());
        currentAction.setOwningUser(actionToUpdate.getOwningUser());
        currentAction.setComplete(actionToUpdate.isComplete());
    }

    @Override
    public void delete(Action oldAction) throws RecordNotFoundException {
        Action currentAction = em.find(Action.class, oldAction.getActionId());
        if (currentAction == null) {
            throw new RecordNotFoundException();
        }

        em.remove(currentAction);
    }
}