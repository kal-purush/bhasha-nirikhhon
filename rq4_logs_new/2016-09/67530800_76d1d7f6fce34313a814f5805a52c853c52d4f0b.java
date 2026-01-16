package cr.pernix.dashboard.services;

import java.io.IOException;
import java.util.Properties;

import javax.ws.rs.core.Response.Status;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.sparkpost.Client;
import com.sparkpost.model.responses.Response;

import cr.pernix.dashboard.utils.ResourceManager;

public class MailService {
    private static volatile MailService instance = null;
    private static final Log LOGGER = LogFactory.getLog(MailService.class);
    static Properties mailProperties;

    private MailService() {

    }

    public static synchronized MailService getInstance() {
        if (instance == null) {
            instance = new MailService();
        }
        return instance;
    }

    static {
        getMailProperties();
    }

    private static void getMailProperties() {
        mailProperties = new Properties();
        try {
            mailProperties.load(ResourceManager.getResourceAsInputStream("mail.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Response sendEmail(String recipient, String subject, String body) {
        Response response = new Response();
        try {
            Client client = new Client(mailProperties.getProperty("mail.api.key"));
            response = client.sendMessage(mailProperties.getProperty("mail.api.email"), recipient, subject, body, body);
            return response;
        } catch (Exception mailerException) {
            LOGGER.error(mailerException);
            mailerException.printStackTrace();
            response.setResponseCode(Status.BAD_REQUEST.getStatusCode());
            return response;
        }
    }
}