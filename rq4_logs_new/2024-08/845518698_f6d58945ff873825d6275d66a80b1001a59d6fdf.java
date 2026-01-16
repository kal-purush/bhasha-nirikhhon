package myApp.service;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class TicketDAO {

    @PersistenceContext
    private EntityManager entityManager;

    @Transactional
    public void saveTicket(Ticket ticket) {
        if (entityManager.contains(ticket)) {
            entityManager.persist(ticket);
        } else {
            entityManager.merge(ticket);
        }
    }
}