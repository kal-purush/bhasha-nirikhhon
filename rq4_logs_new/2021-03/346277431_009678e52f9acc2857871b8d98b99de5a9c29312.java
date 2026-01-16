package hr.tvz.keepthechange;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.user;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.model;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.view;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import hr.tvz.keepthechange.entity.Transaction;
import hr.tvz.keepthechange.service.TransactionService;

@SpringBootTest
@AutoConfigureMockMvc
public class ExpensesTest {

	@Autowired
	private MockMvc mockMvc;
		
	@Test
	public void testGetMethod() throws Exception {
		this.mockMvc
			.perform(get("/transaction/new").with(user("admin").password("adminpass")
			.roles("USER", "ADMIN")))
			.andExpect(status().isOk())
			.andExpect(model().attributeExists("transactionDto"))
			.andExpect(model().attributeExists("type"))
            .andExpect(model().attributeExists("category"))
			.andExpect(view().name("transaction"));
	}
	
	@Test
	public void testPostMethodTransaction() throws Exception {
		
		//test for errors
		this.mockMvc
		.perform(post("/transaction/new").with(user("admin").password("adminpass")
		.roles("USER", "ADMIN")).with(csrf()))
		.andExpect(status().isOk())
		.andExpect(model().attributeExists("transactionDto"))
		.andExpect(view().name("transaction"));
		this.mockMvc
			.perform(post("/transaction/new").with(user("admin").password("adminpass")
			.roles("USER", "ADMIN")).with(csrf())
			.param("name", "test")
			.param("transactionType", "Uplata")
            .param("transactionCategory", "Namirnice")
			.param("value", "50"))
			.andExpect(status().isOk())
            .andExpect(model().attributeExists("transaction"))
			.andExpect(model().attributeExists("wallet"))
			.andExpect(view().name("transactionInfo"));
            
	}
	
    @Test
	public void testPostMethodExpense() throws Exception {
		
		//test for errors
		this.mockMvc
		.perform(post("/transaction/new").with(user("admin").password("adminpass")
		.roles("USER", "ADMIN")).with(csrf()))
		.andExpect(status().isOk())
		.andExpect(model().attributeExists("transactionDto"))
		.andExpect(view().name("transaction"));
		this.mockMvc
			.perform(post("/transaction/new").with(user("admin").password("adminpass")
			.roles("USER", "ADMIN")).with(csrf())
			.param("name", "testTrosak")
			.param("transactionType", "Trošak")
            .param("transactionCategory", "Namirnice")
			.param("value", "50"))
			.andExpect(status().isOk())
			.andExpect(model().attributeExists("transaction"))
			.andExpect(model().attributeExists("wallet"))
			.andExpect(view().name("transactionInfo"));
	}

}