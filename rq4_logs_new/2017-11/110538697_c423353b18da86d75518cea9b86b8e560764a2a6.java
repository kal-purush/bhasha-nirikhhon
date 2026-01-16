package ax.test.archetype.core.servlets;

import ax.test.archetype.core.servlets.configuration.ConfigurationNewPostServlet;
import static org.apache.sling.api.servlets.ServletResolverConstants.SLING_SERVLET_METHODS;
import static org.apache.sling.api.servlets.ServletResolverConstants.SLING_SERVLET_PATHS;

//import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.rmi.ServerException;

import javax.jcr.Node;
import javax.jcr.Session;
import javax.servlet.Servlet;

import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.SlingHttpServletResponse;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.api.servlets.HttpConstants;
import org.apache.sling.commons.json.JSONObject;
import org.apache.sling.jcr.api.SlingRepository;
import org.osgi.framework.Constants;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.metatype.annotations.Designate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(service = Servlet.class,
        property = {
            Constants.SERVICE_DESCRIPTION + " = Simplre POST Servlet",
            SLING_SERVLET_PATHS + "=/bin/new-post-servlet",
            SLING_SERVLET_METHODS + "=" + HttpConstants.METHOD_POST
        }, configurationPolicy = ConfigurationPolicy.IGNORE
)
@Designate(ocd = ConfigurationNewPostServlet.class)
public class NewPostServlet extends org.apache.sling.api.servlets.SlingAllMethodsServlet {

    private static final long serialVersionUID = 2598426539166789515L;

    private String CATALOG_PATH;
    private String DAM_PATH;
    private String REPOSITORY_PASSWORD;
    private Node bcNode;

    ResourceResolver resourceResolver;
    Session session;

    @Reference
    private SlingRepository repository;

    //Inject a Sling ResourceResolverFactory
    @Reference
    private ResourceResolverFactory resolverFactory;

    /**
     * Default log.
     */
    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    @Activate
    protected void activate(ConfigurationNewPostServlet config) {
        log.debug("MARK  doPost activate ------------------------------------------------------------------");
        this.CATALOG_PATH = config.catalogPath();
        this.DAM_PATH = config.damPath();
        this.REPOSITORY_PASSWORD = config.repositoryPassword();
    }

    /**
     * @param request
     * @param response
     * @Override protected void doGet(SlingHttpServletRequest request,
     * SlingHttpServletResponse response) throws ServerException, IOException {
     * doPost(request, response); }
    **
     */
    @Override
    protected void doPost(SlingHttpServletRequest request, SlingHttpServletResponse response) 
            throws ServerException, IOException {

        log.debug("MARK  doPost entry ------------------------------------------------------------------");

        response.setCharacterEncoding("UTF-8");
        response.setContentType("application/json");

        PrintWriter out = response.getWriter();
        JSONObject json = new JSONObject();

        try {
            json.put("status", "OK");
            json.put("message", "POST Received");
            out.write(json.toString());
        } catch (Exception e) {
            log.error("MARK Simple POST Servlet - Error - " + e.getMessage());
            // e.printStackTrace();
            try {
                json.put("status", "Error");
                json.put("message", e.getMessage());
                json.put("path", "");
                out.write(json.toString());
            } catch (Exception ex) {
                log.error("error: {}", ex.getMessage());

            }
        }

        out.flush();

    }
}