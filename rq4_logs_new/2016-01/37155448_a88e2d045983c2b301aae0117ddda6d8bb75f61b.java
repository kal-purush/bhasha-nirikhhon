package pl.salonea.jaxrs;

import pl.salonea.entities.TermEmployeeWorkOn;
import pl.salonea.jaxrs.bean_params.GenericBeanParam;
import pl.salonea.jaxrs.exceptions.ForbiddenException;
import pl.salonea.jaxrs.exceptions.NotFoundException;
import pl.salonea.jaxrs.utils.hateoas.Link;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.lang.reflect.Method;

/**
 * Created by michzio on 26/01/2016.
 */
@Path("/employee-terms")
public class EmployeeTermResource {


    @GET
    @Path("/{employeeId: \\d+}+{termId: \\d+}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public Response getEmployeeTerm(  @PathParam("employeeId") Long employeeId,
                                      @PathParam("termId") Long termId,
                                      @BeanParam GenericBeanParam params ) throws ForbiddenException, NotFoundException {

        return null;
    }


    /**
     * This method enables to populate each individual resource with hypermedia links
     */
    public static void populateWithHATEOASLinks(TermEmployeeWorkOn employeeTerm, UriInfo uriInfo) {

        try {
            // self link with pattern: http://localhost:port/app/rest/{resources}/{id1}+{id2}
            Method employeeTermMethod = EmployeeTermResource.class.getMethod("getEmployeeTerm", Long.class, Long.class, GenericBeanParam.class);
            employeeTerm.getLinks().add(Link.fromUri(uriInfo.getBaseUriBuilder()
                    .path(EmployeeTermResource.class)
                    .path(employeeTermMethod)
                    .resolveTemplate("employeeId", employeeTerm.getEmployee().getUserId().toString())
                    .resolveTemplate("termId", employeeTerm.getTerm().getTermId().toString())
                    .build())
                    .rel("self").build());

        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
    }
}