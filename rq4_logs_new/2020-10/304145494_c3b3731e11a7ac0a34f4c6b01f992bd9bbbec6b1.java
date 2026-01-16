package wolox.training.controllers;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import wolox.training.models.Book;
import wolox.training.repositories.BookRepository;
import wolox.training.repositories.UsersRepository;

import java.util.Optional;

import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = BookController.class)
public class BookControllerTest {

    @Autowired
    private MockMvc mvc;

    @MockBean
    private UsersRepository mockUsersRepository;
    @MockBean
    private BookRepository mockBookRepository;

    private static Book oneTestBook;
    private static final String USER_PATH = "/api/books";

    @BeforeAll
    static void setUp() {

        oneTestBook = new Book();
        oneTestBook.setId(1l);

    }

    @Test
    void whenFindBookByIdThenReturnStatusOK() throws Exception {
        given(mockBookRepository.findById(1L)).willReturn(Optional.of(oneTestBook));
        String url = (USER_PATH + "/1");
        mvc.perform(get(url)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());
    }
}