package hr.tvz.keepthechange;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.user;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.model;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.view;

import javax.persistence.Index;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import hr.tvz.keepthechange.controller.IndexController;
import hr.tvz.keepthechange.entity.Transaction;
import hr.tvz.keepthechange.enumeration.TransactionCategory;
import hr.tvz.keepthechange.enumeration.TransactionType;
import hr.tvz.keepthechange.service.TransactionService;


@SpringBootTest
@AutoConfigureMockMvc
public class HomeScreenTest {

	@Autowired
	private MockMvc mockMvc;



	@Test
	public void testGetMethod() throws Exception {
		this.mockMvc
			.perform(get("/index").with(user("admin").password("adminpass")
			.roles("USER", "ADMIN")))
			.andExpect(status().isOk())
			.andExpect(model().attributeExists("walletStatus"))
			.andExpect(model().attributeExists("transactionCategories"))
			.andExpect(model().attributeExists("transactions"))
			.andExpect(view().name("index"));
	}
		
	@Test
	public void testFilterName() throws Exception{
		
		this.mockMvc
			.perform(get("/index").with(user("admin").password("adminpass")
			.roles("USER", "ADMIN")).with(csrf())
			.param("name", "vecera"))
			.andExpect(status().isOk()).andExpect(model().attributeExists("transactions"))
			.andExpect(model().attributeExists("walletStatus"))
			.andExpect(model().attributeExists("transactionCategories"))
			.andExpect(view().name("index"));
	}


	@Test
	public void testFilterCategory() throws Exception{
		
		this.mockMvc
			.perform(get("/index").with(user("admin").password("adminpass")
			.roles("USER", "ADMIN")).with(csrf())
			.param("category", TransactionCategory.GROCERIES.toString()))
			.andExpect(status().isOk()).andExpect(model().attributeExists("transactions"))
			.andExpect(model().attributeExists("walletStatus"))
			.andExpect(model().attributeExists("transactionCategories"))
			.andExpect(view().name("index"));	
		
	}

	@Test
	public void testFilterDate() throws Exception{
		
		this.mockMvc
			.perform(get("/index").with(user("admin").password("adminpass")
			.roles("USER", "ADMIN")).with(csrf())
			.param("date", "2021-03-27"))
			.andExpect(status().isOk()).andExpect(model().attributeExists("transactions"))
			.andExpect(model().attributeExists("walletStatus"))
			.andExpect(model().attributeExists("transactionCategories"))
			.andExpect(view().name("index"));	
		
	}


}