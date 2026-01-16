package hello;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.xpath;

import org.springframework.security.oauth2.client.registration.ClientRegistrationRepository;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.security.oauth2.core.user.OAuth2User;
import org.junit.Before;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.authentication;


@RunWith(SpringRunner.class)
@WebMvcTest(WebController.class)
public class EarthquakeSearchTest {

    @Autowired
    private MockMvc mvc;

    @MockBean
    private AuthControllerAdvice aca;

    @MockBean
    private ClientRegistrationRepository crr;

    private OAuth2User principal;

    /**
     * Set up an OAuth mock user so that we can unit test protected endpoints
     */
    @Before
    public void setUpUser() {
        principal = OAuthUtils.createOAuth2User("Chris Gaucho", "cgaucho@example.com");
    }

    @Test
    @WithMockUser
    public void getEarthquakeSearch() throws Exception {
        mvc.perform(MockMvcRequestBuilders.get("/earthquakes/search")
            .with(authentication(OAuthUtils.getOauthAuthenticationFor(principal)))
            .accept(MediaType.TEXT_HTML))
            .andExpect(status().isOk())
            .andExpect(xpath("//title").exists())
            .andExpect(xpath("//title").string("Earthquake Search"));
    }
}