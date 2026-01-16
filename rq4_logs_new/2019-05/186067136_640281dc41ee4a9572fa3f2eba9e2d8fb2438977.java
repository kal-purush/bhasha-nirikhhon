package li.antonio.ci_requirement.echo;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("greetings")
public class GreetingResource {
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String greetMe() {
        return "Hello World";
    }

}