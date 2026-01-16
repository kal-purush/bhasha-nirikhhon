package mybase.tools;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletResponse;

@Slf4j
@Service
public class LoginResponseProcessor {

    private final String SUCCESSFUL_RESPONSE_MESSAGE = "Login successful!";


    public void processSuccessfulResponse(HttpServletResponse response) {
        try {
            log.info("processSuccessfulResponse");
            // JsonHelper.createJsonFrom(body).getBytes( StandardCharsets.UTF_8 )
            response.setStatus(202);
            response.addHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=UTF-8");
            response.getWriter().write(SUCCESSFUL_RESPONSE_MESSAGE);
        }
        catch (Exception e) {
            log.error("", e);

        }
        finally {

            log.warn("finally");

        }
    }
}