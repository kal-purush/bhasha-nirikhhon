package ro.gov.ithub.stopcozi.api.broadcast;

import io.dropwizard.hibernate.UnitOfWork;
import ro.gov.ithub.stopcozi.api.broadcast.model.Agency;
import ro.gov.ithub.stopcozi.api.broadcast.model.Ticket;
import ro.gov.ithub.stopcozi.api.broadcast.resource.AgenciesApi;

import javax.inject.Singleton;
import javax.ws.rs.core.Response;

@Singleton
public class AgenciesApiImpl implements AgenciesApi{

    @Override
    @UnitOfWork
    public Response createTicket(String agencyId, String serviceId, Ticket ticket) {
        return null;
    }

    @Override
    @UnitOfWork
    public Response updateAgency(String agencyId, Agency agency) {
        return null;
    }

    @Override
    @UnitOfWork
    public Response updateTicket(String agencyId, String serviceId, String ticketNo, Ticket ticket) {
        return null;
    }
}