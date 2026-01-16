package help.desk.mobile.api.service.ticket;

import help.desk.mobile.api.domain.entity.TicketEntity;
import help.desk.mobile.api.repository.ticket.TicketRepository;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class FindAllTicketsByAuthorIdService {

    private TicketRepository ticketRepository;

    public FindAllTicketsByAuthorIdService(TicketRepository ticketRepository) {
        this.ticketRepository = ticketRepository;
    }

    public List<TicketEntity> findAllByAuthorId(Long authorId){
        return ticketRepository.findAllByAuthorId(authorId);
    }
}