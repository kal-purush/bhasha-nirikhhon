package pw2.webservice.openliberty.km.pw2.webservice.openliberty.km;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 *
 */
@Path("/convert")
@Singleton
public class HelloController {

    private static final double kmhMph = 0.621371;
    private static final double nosKmh = 1.852;

    //Quilômetro por hora para milhas por hora (1=0.621371)
    // – esse método deve consumir por POST e
    // produzir dados em texto.
    @POST
    @Path("/kmhForMph")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.APPLICATION_JSON)
    public double convertKmhMph (@FormParam("km") final double km)
    {
        return kmhMph * km;
    }

    // Nós para quilometro por hora (1=1.852)
    // – esse método deve consumir dados por GET e
    // produzir dados em em JSON.
    @GET
    @Path("/nosParaKmh/{nos}")
    @Produces(MediaType.APPLICATION_JSON)
    public double convertNosKmh(@PathParam("nos") final double nos)
    {
        return nosKmh * nos;
    }    
}