package com.vk_media.vkmedia;

import com.mongodb.client.MongoCollection;
import com.vk_media.vkmedia.controller.ExceptionHandlerController;
import com.vk_media.vkmedia.dto.PhotoWithImage;
import com.vk_media.vkmedia.exception.VkUnauthenticatedException;
import com.vk_media.vkmedia.service.MongoPhotoService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.*;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@RunWith(SpringRunner.class)
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.MOCK,
        classes = VkMediaApplication.class
)
@AutoConfigureMockMvc
@Testcontainers
@ContextConfiguration(initializers = {VkMediaIntegrationTest.Initializer.class})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class VkMediaIntegrationTest {

    @Autowired
    MockMvc mockMvc;

    @Autowired
    MongoPhotoService mongoPhotoService;

    @Autowired
    MongoCollection<PhotoWithImage> mongoPhotoCollection;

    private final static int MONGO_PORT = 27017;

    @Container
    public static GenericContainer<?> mongo = new GenericContainer<>(DockerImageName.parse("mongo"))
            .withExposedPorts(MONGO_PORT);

    static class Initializer
            implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            TestPropertyValues.of(
                    "spring.data.mongodb.uri=mongodb://"+ mongo.getHost() + ":" + MONGO_PORT + "/vkmedia"
            ).applyTo(configurableApplicationContext.getEnvironment());
        }
    }

    @BeforeAll
    public void setUp() {
        PhotoWithImage image1 = new PhotoWithImage(
                ObjectId.get(),
                "1",
                10,
                "http://test.com/previewImgUrl1.jpg",
                "http://test.com/imgUrl1.jpg",
                "Btag Ztag Mtag Atag");
        PhotoWithImage image2 = new PhotoWithImage(
                ObjectId.get(),
                "2",
                10,
                "http://test.com/previewImgUrl2.jpg",
                "http://test.com/imgUrl2.jpg",
                "Qtag Atag Mtag");
        mongoPhotoService.addPhotoWithTag(image1);
        mongoPhotoService.addPhotoWithTag(image2);
    }

    @Test
    public void givenUnauthorizedSession_WhenGetAlbumCall_ThenRedirecteWithErrorMessage() throws Exception {
        mockMvc.perform(get("/albums/1").contentType(MediaType.APPLICATION_JSON))
                .andExpect(result -> assertTrue(result.getResolvedException() instanceof VkUnauthenticatedException))
                .andExpect(status().isFound())
                .andExpect(redirectedUrl("/"))
                .andExpect(flash().attribute("error", ExceptionHandlerController.NOT_AUTHORIZED_IN_VK));
    }

    @Test
    public void givenImagesWithDifferentTags_WhenCallGetExistingTagsRest_ThenGetExistingTags() throws Exception {
        mockMvc.perform(get("/photos/getExistingTagsRest").contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(result ->
                        assertEquals("[\"Atag\",\"Btag\",\"Mtag\",\"Qtag\",\"Ztag\"]", result.getResponse().getContentAsString()));
    }

    @Test
    public void givenImagesWithDifferentTags_WhenCallGetPhotosByTagRest_ThenGetPhoto() throws Exception {
        mockMvc.perform(get("/photos/getPhotosByTagRest")
                .param("tags", "Btag")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(1)))
                .andExpect(jsonPath("$[0].vkId", is("1")));
    }

    @AfterAll
    public void cleanUp() {
        mongoPhotoCollection.drop();
    }
}