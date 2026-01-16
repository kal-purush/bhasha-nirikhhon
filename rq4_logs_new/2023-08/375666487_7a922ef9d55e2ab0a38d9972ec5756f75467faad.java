package uk.gov.hmcts.reform.pip.channel.management.services.helpers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.ActiveProfiles;

import static org.junit.jupiter.api.Assertions.assertEquals;


@ActiveProfiles("test")
class SittingHelperTest {

    private static ObjectMapper objectMapper = new ObjectMapper();

    private static String nodeWithJudiciary = """
        {
          "judiciary": [
            {
              "johKnownAs": "This is a known as"
            }
          ]
        }
        """;

    private static String courtRoom = """
        {
          "courtRoomName": "This is a court room name"
        }
        """;

    private JsonNode nodeWithJudiciaryJson;
    private JsonNode courtRoomJson;

    private JsonNode nodeWithoutJudiciaryJson;

    private static final String DESTINATION_NODE_NAME = "This is a destination node name";

    @BeforeEach
    public void setup() throws JsonProcessingException {
        nodeWithJudiciaryJson = objectMapper.readTree(nodeWithJudiciary);
        courtRoomJson = objectMapper.readTree(courtRoom);
        nodeWithoutJudiciaryJson = objectMapper.createObjectNode();
    }

    @Test
    void testManipulatedSittingWithSitting() {
        SittingHelper.manipulatedSitting(courtRoomJson, nodeWithoutJudiciaryJson,
                                         nodeWithJudiciaryJson, DESTINATION_NODE_NAME
        );

        assertEquals("This is a court room name: This is a known as",
                     nodeWithoutJudiciaryJson.get(DESTINATION_NODE_NAME).asText(),
                     "Correct court room name not shown");
    }

    @Test
    void testManipulatedSittingWithSession() {
        SittingHelper.manipulatedSitting(courtRoomJson, nodeWithJudiciaryJson,
                                         nodeWithoutJudiciaryJson, DESTINATION_NODE_NAME
        );

        assertEquals("This is a court room name: This is a known as",
                     nodeWithJudiciaryJson.get(DESTINATION_NODE_NAME).asText(),
                     "Correct court room name not shown");
    }




}
