package com.vsocolov.roomba.api.resource;

import com.vsocolov.roomba.api.PathConstants.Resources;
import com.vsocolov.roomba.data.request.CleaningRequest;
import com.vsocolov.roomba.data.response.CleaningResponse;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.client.RestClientException;

import static com.vsocolov.roomba.api.PathConstants.ROOT_PATH;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
public class CleaningResourceTest {

    private static final String RESOURCE = ROOT_PATH + Resources.CLEAN;

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    public void clean_should_pass_validation_and_clean_the_room() {
        final CleaningRequest request = dummyRequest();

        final ResponseEntity<CleaningResponse> responseEntity = restTemplate.postForEntity(RESOURCE, request,
                CleaningResponse.class);

        assertThat(responseEntity.getStatusCode(), equalTo(HttpStatus.OK));
        assertThat(responseEntity.getBody().getPatches(), equalTo(1));
        assertThat(responseEntity.getBody().getCoords(), equalTo(new Integer[]{1, 3}));
    }

    @Test(expected = RestClientException.class)
    public void clean_should_not_pass_validation_if_instructions_are_null() {
        final CleaningRequest request = dummyRequest();
        request.setInstructions(null);


        restTemplate.postForEntity(RESOURCE, request, CleaningResponse.class);
    }

    private CleaningRequest dummyRequest() {
        final CleaningRequest request = new CleaningRequest();
        request.setRoomSize(new Integer[]{5, 5});
        request.setCoords(new Integer[]{1, 2});
        request.setPatches(new Integer[][]{{1, 0}, {2, 2}, {2, 3}});
        request.setInstructions("NNESEESWNWW");

        return request;
    }
}