package com.lohika.morning.ecs.domain.event;

import com.lohika.morning.ecs.BaseControllerTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.ResultActions;

import static org.springframework.restdocs.mockmvc.MockMvcRestDocumentation.document;
import static org.springframework.restdocs.mockmvc.RestDocumentationRequestBuilders.get;
import static org.springframework.restdocs.payload.PayloadDocumentation.fieldWithPath;
import static org.springframework.restdocs.payload.PayloadDocumentation.responseFields;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles("test")
public class EventControllerTest extends BaseControllerTest {

    @Test
    public void testGetEvents_unauthorized() throws Exception {
        mockMvc.perform(get("/api/events")
                .header("Authorization", "Basic wRongPASsWorD="))
                .andDo(print())
                .andExpect(status().isUnauthorized());
    }

    @Test
    public void testGetEvents_emptyDatabase() throws Exception {
        mockMvc.perform(get("/api/events"))
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(content().contentType(HAL_JSON_CHARSET_UTF_8));
                // TODO: verify links
                //.andExpect(content().string("{}"));
    }

    @Test
    public void testGetEvents_found() throws Exception {
        Event event = given.event("Golang").save();
        Event event2 = given.event("Robotic").save();

        // When
        ResultActions actions = mockMvc.perform(get("/api/events"))
                .andDo(print());

        // Then
        actions.andExpect(status().isOk())
                .andExpect(content().contentType(HAL_JSON_CHARSET_UTF_8));
        // TODO: match all events
        //.andExpect(eventsMatches(Arrays.asList(event, event2)));

        actions.andDo(document("{class-name}/{method-name}",
                responseFields(
                        fieldWithPath("_embedded").description("'event' array with event resources."),
                        fieldWithPath("_embedded.events").description("Array with returned Event resources."),
                        fieldWithPath("_embedded.events[].name").description("Event name."),
                        fieldWithPath("_embedded.events[].description").description("Event description."),
                        fieldWithPath("_embedded.events[].date").description("Event date."),
                        fieldWithPath("_embedded.events[]._links").description("Event links."),
                        fieldWithPath("_embedded.events[]._links.self.href").description("Link to self."),
                        fieldWithPath("_embedded.events[]._links.event.href").description("Link to self."),
                        fieldWithPath("_links").description("Collection related links"),
                        fieldWithPath("page").description("Paging information: page size, total element, total pages, number")
                )));
    }
}