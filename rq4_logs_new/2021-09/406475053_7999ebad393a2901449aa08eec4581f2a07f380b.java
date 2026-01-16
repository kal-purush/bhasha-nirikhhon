package ca.bc.gov.open.Scss;

import static org.mockito.Mockito.when;

import ca.bc.gov.open.Scss.Controllers.HealthController;
import com.example.demp.wsdl.*;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.client.RestTemplate;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
public class HealthControllerTests {

    @Mock private RestTemplate restTemplate;

    @Test
    public void getHealthTest() {
        HealthController healthController = new HealthController(restTemplate);

        var hr = new GetHealthResponse();
        hr.setAppid("A");
        hr.setCompatibility("A");
        hr.setHost("A");
        hr.setMethod("A");
        hr.setVersion("A");
        hr.setInstance("A");
        hr.setStatus("A");

        ResponseEntity<GetHealthResponse> responseEntity = new ResponseEntity<>(hr, HttpStatus.OK);

        //     Set up to mock ords response
        when(restTemplate.exchange(
                        Mockito.any(String.class),
                        Mockito.eq(HttpMethod.GET),
                        Mockito.<HttpEntity<String>>any(),
                        Mockito.<Class<GetHealthResponse>>any()))
                .thenReturn(responseEntity);

        GetHealthResponse out = healthController.getHealth(new GetHealth());

        assert out != null;
    }

    @Test
    public void getPingTest() {
        HealthController healthController = new HealthController(restTemplate);

        var pr = new GetPingResponse();
        pr.setStatus("A");

        ResponseEntity<GetPingResponse> responseEntity = new ResponseEntity<>(pr, HttpStatus.OK);

        //     Set up to mock ords response
        when(restTemplate.exchange(
                        Mockito.any(String.class),
                        Mockito.eq(HttpMethod.GET),
                        Mockito.<HttpEntity<String>>any(),
                        Mockito.<Class<GetPingResponse>>any()))
                .thenReturn(responseEntity);

        GetPingResponse out = healthController.getPing(new GetPing());

        assert out != null;
    }
}