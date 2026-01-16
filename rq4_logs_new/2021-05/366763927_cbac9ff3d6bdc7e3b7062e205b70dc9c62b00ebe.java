package at.lumbardh.swlcm.maintenance.monitor;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/api/maintenance monitor/")
public class MaintenanceMonitor {

    @GET
    @Path("/{sourceString}")
    @Produces(MediaType.TEXT_PLAIN)
    public String GetMaintenanceMonitorStringFromSourceString(@PathParam("sourceString") String sourceString) {
        return sourceString;
    }
}