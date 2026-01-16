package bloodbank.bloodbankservice.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.servers.Server;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

// TODO: Refactor code if necessary and clean up later.

@Configuration
public class SwaggerConfiguration {
    //region Constant Fields
    private static final String API_VERSION = "1.0.0";
    private static final String API_TITLE = "Blood Bank API";
    private static final String API_LICENSE = "MIT";
    private static final String API_DESCRIPTION = "API For managing blood banks, donors, and blood stock items.";

    @Value("${blood-bank.dev.url}")
    private String devUrl;

    @Value("${blood-bank.prod.url}")
    private String prodUrl;
    //endregion

    private List<Contact> contacts = List.of(
            createContact("Benjamin Lefebvre", "<github url>", "<email address>"),
            createContact("Chris Busse", "<github url>", "<email address>"),
            createContact("Abdoullahi Isse", "<github url>", "<email address>"),
            createContact("Muhammad Bilal Khan", "https://github.com/Hi-kue", "lofi.audit@gmail.com")
    );

    @Bean
    public OpenAPI openAPI() {
        Server dev = new Server()
                .url(devUrl)
                .description("Development Server for Blood Bank API");

        Contact contact = new Contact()
                .name("Muhammad Bilal Khan")
                .url("https://github.com/Hi-kue")
                .email("lofi.audit@gmail.com");

        Info info = new Info()
                .title(API_TITLE)
                .version(API_VERSION)
                .description(API_DESCRIPTION)
                .license(new License()
                        .name(API_LICENSE)
                        .url("https://choosealicense.com/licenses/mit/"))
                .contact(contact);



        return new OpenAPI().info(info).addServersItem(dev);
    }

    //region Private Methods
    @Deprecated
    private Info createInfo(String... args) {
        return new Info()
                .title(Strings.isNotBlank(args[0]) ? args[0] : API_TITLE)
                .version(Strings.isNotBlank(args[1]) ? args[1] : API_VERSION)
                .description(Strings.isNotBlank(args[2]) ? args[2] : API_DESCRIPTION)
                .license(new License()
                        .name(Strings.isNotBlank(args[3]) ? args[3] : API_LICENSE));
    }

    @Deprecated
    private Contact createContact(String name, String url, String email) {
        return new Contact()
                .name(name)
                .url(url)
                .email(email);
    }
    //endregion
}