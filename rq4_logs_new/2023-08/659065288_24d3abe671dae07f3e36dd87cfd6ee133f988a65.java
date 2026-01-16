package kr.rogarithm.todos.global.auth;

import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import javax.servlet.http.Cookie;
import kr.rogarithm.todos.domain.todo.controller.TodoController;
import kr.rogarithm.todos.domain.todo.service.TodoService;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

@WebMvcTest(controllers = TodoController.class)
class JwtAuthenticationFilterTest {

    @Autowired
    MockMvc mockMvc;

    @InjectMocks
    TodoController todoController;

    @MockBean
    TodoService todoService;

    @MockBean
    JwtAuthenticationManager jwtAuthenticationManager;

    @Test
    public void failWithUnauthorizedCodeWhenTokenIsNull() throws Exception {

        Long todoId = 1L;

        MockHttpServletRequestBuilder requestNoAuthHeader = MockMvcRequestBuilders.get("/todo/{todoId}", todoId)
                                                                                  .cookie(new Cookie("RefreshToken", "refresh-token"));

        this.mockMvc.perform(requestNoAuthHeader)
                    .andDo(print())
                    .andExpect(status().isUnauthorized());
    }
}