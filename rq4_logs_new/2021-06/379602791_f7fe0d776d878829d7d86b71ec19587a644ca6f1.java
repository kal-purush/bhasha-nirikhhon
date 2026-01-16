package customer.central_mta_backend.it;

import static org.hamcrest.CoreMatchers.containsString;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import customer.central_mta_backend.Application;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
@AutoConfigureMockMvc
public class CatalogServiceITestV4 {

	@Autowired
	private MockMvc mockMvc;

	@Test
	public void testReadBooks() throws Exception {
		mockMvc.perform(MockMvcRequestBuilders.get("/odata/v4/CatalogService/Books")).andExpect(status().isOk())
				.andExpect(jsonPath("$.value[0].title").value(containsString("Wuthering Heights")))
				.andExpect(jsonPath("$.value[0].stock").value(100))
				.andExpect(jsonPath("$.value[1].title").value(containsString("Jane Eyre (discounted)")))
				.andExpect(jsonPath("$.value[1].stock").value(500));
	}
}