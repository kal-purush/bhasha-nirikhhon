package jaeseok.numble.mybox.folder.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import jaeseok.numble.mybox.common.response.ResponseCode;
import jaeseok.numble.mybox.folder.dto.FolderCreateParam;
import jaeseok.numble.mybox.member.dto.LoginResponse;
import jaeseok.numble.mybox.util.annotation.IntegrationTest;
import jaeseok.numble.mybox.util.initializer.TestInitializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;

import static org.hamcrest.Matchers.is;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@IntegrationTest
public class FolderControllerTest {

    @Autowired
    protected MockMvc mockMvc;

    @Autowired
    protected ObjectMapper objectMapper;

    @Autowired
    TestInitializer authorizedTestInitializer;

    private final String email = "test_email@test.com";

    private final String password = "test_password";

    private String jwt;

    private Long rootFolderId;

    @BeforeEach
    void initialize() throws Exception {
        LoginResponse loginResponse = authorizedTestInitializer
                .memberInitialize(mockMvc, objectMapper, email, password);

        this.jwt = loginResponse.getJwt();
        this.rootFolderId = loginResponse.getRootFolderId();
    }

    @Nested
    @DisplayName("폴더 생성")
    class CreateFolder {

        @Test
        @DisplayName("성공")
        void success() throws Exception {
            // given
            String folderName = "테스트 폴더";
            FolderCreateParam folderCreateParam = new FolderCreateParam(rootFolderId, folderName);

            // when
            ResultActions resultActions = mockMvc.perform(post("/api/v1/folder")
                            .contentType(MediaType.APPLICATION_JSON)
                            .header("Authorization", jwt)
                            .content(objectMapper.writeValueAsString(folderCreateParam))
                            .accept(MediaType.APPLICATION_JSON))
                    .andDo(print());

            // then
            resultActions
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("success").value(true))
                    .andExpect(jsonPath("errorCode").value(ResponseCode.SUCCESS.getCode()))
                    .andExpect(jsonPath("content.id").isNumber())
                    .andExpect(jsonPath("content.name", is(folderName)))
                    .andExpect(jsonPath("content.createdAt").isNotEmpty())
                    .andExpect(jsonPath("content.modifiedAt").isNotEmpty());
        }

        @Test
        @DisplayName("하나의 폴더 내부에 같은 이름의 폴더를 생성하려는 경우 실패")
        void fail_to_duplicated_named_folder() throws Exception {
            // given
            String folderName = "테스트 폴더";
            FolderCreateParam folderCreateParam = new FolderCreateParam(rootFolderId, folderName);

            // 폴더 생성
            mockMvc.perform(post("/api/v1/folder")
                            .contentType(MediaType.APPLICATION_JSON)
                            .header("Authorization", jwt)
                            .content(objectMapper.writeValueAsString(folderCreateParam))
                            .accept(MediaType.APPLICATION_JSON))
                    .andDo(print());

            // when
            ResultActions resultActions = mockMvc.perform(post("/api/v1/folder")
                            .contentType(MediaType.APPLICATION_JSON)
                            .header("Authorization", jwt)
                            .content(objectMapper.writeValueAsString(folderCreateParam))
                            .accept(MediaType.APPLICATION_JSON))
                    .andDo(print());

            // then
            resultActions
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("success").value(false))
                    .andExpect(jsonPath("errorCode").value(ResponseCode.FOLDER_NAME_EXIST.getCode()))
                    .andExpect(jsonPath("content").value(ResponseCode.FOLDER_NAME_EXIST.getMessage()));
        }
    }
}