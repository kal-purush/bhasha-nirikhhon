package hdn.projects.gestiondevis.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import hdn.projects.gestiondevis.entities.Client;
import hdn.projects.gestiondevis.entities.Devis;
import hdn.projects.gestiondevis.service.IGestion;
import hdn.projects.gestiondevis.utils.EtatOperation;

@ExtendWith(SpringExtension.class)
@WebMvcTest(controllers = GestionDevisController.class)
@AutoConfigureMockMvc
@TestPropertySource(locations = "classpath:test.properties")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DevisControllerIntegrationTest {

	@Autowired
	private MockMvc mockMvc;

	@MockBean
	IGestion devisService;

	@Value("${controller.devisPath}")
	private String devisControllerPath;

	@Value("${date.format}")
	String dateFormat;
	
	private static final ObjectMapper objectMapper = new ObjectMapper();

	private String getURL(String uri) {
		uri = devisControllerPath + uri;
		return uri;
	}

	private static List<Devis> liste = new ArrayList<Devis>();

	@BeforeAll
	public static void init() {
		Client client = new Client("john", "doe", "johnDoe", "123456");
		for(int i=0; i <10; i++) {
			Devis devis = genererDevis();
			devis.setReference(Long.valueOf(i));
			liste.add(devis);
		}
		liste.get(0).setClient(client);
		
		//  configure Jackson to map a LocalDate into a String
		objectMapper.registerModule(new JavaTimeModule());
		objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

	}

	@Test
	@Order(1)
	@WithMockUser(username = "admin", password = "admin")
	public void test_getDevis() throws Exception {

		// prepare data and mock's behaviour
		when(devisService.getEntities()).thenReturn(liste);

		// execute
		MvcResult result = mockMvc
				.perform(MockMvcRequestBuilders.get(getURL("liste")).accept(MediaType.APPLICATION_JSON)).andReturn();

		// verify
		int status = result.getResponse().getStatus();

		assertTrue(status == HttpStatus.OK.value() || status == HttpStatus.FOUND.value());

		// verify that service method was called once
		verify(devisService).getEntities();
		
		List<Devis> listeResult = Arrays
				.asList(objectMapper.readValue(result.getResponse().getContentAsString(), Devis[].class));

		assertNotNull(listeResult, "Aucun devis");
		assertEquals(liste.size(), listeResult.size(), "Incorrect Devis List");
	}

	@Test
	@Order(2)
	@WithMockUser(username = "admin", password = "admin")
	public void test_saveDevis() throws Exception {

		when(devisService.saveOrUpdateEntity(any(Devis.class), any(EtatOperation.class))).thenReturn(liste.get(0));

		// execute
		MvcResult result = mockMvc.perform(MockMvcRequestBuilders.post(getURL("liste"))
				.with(csrf()) // sinon 403
				.content(objectMapper.writeValueAsString(liste.get(0)))
				.header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON))
				.andExpect(status().isCreated())
				.andReturn();

		int status = result.getResponse().getStatus();
		assertEquals(HttpStatus.CREATED.value(), status, "Incorrect Response Status");

		Devis resultDevis = objectMapper.readValue(result.getResponse().getContentAsString(), Devis.class);
		assertNotNull(resultDevis);

		// verify that service method was called once
		verify(devisService).saveOrUpdateEntity(any(Devis.class), any(EtatOperation.class));
	}

	@Test
	@Order(3)
	@WithMockUser(username = "admin", password = "admin")
	public void test_findDevisByReference() throws Exception {

		when(devisService.getEntityWithOptional(any(Long.class))).thenReturn(liste.get(0));

		LocalDate dateJour = LocalDate.now();
		String dateJourString = dateJour.format(DateTimeFormatter.ofPattern(this.dateFormat));
		
		// execute
		MvcResult result = mockMvc.perform(MockMvcRequestBuilders.get(getURL("liste/1"))
				.accept(MediaType.APPLICATION_JSON))
				.andExpect((status().isFound()))
				.andExpect(content()
				.contentType(MediaType.APPLICATION_JSON))
				.andExpect(jsonPath("$.dateDevis").value(dateJourString))
				.andReturn();

		Devis resultDevis = objectMapper.readValue(result.getResponse().getContentAsString(), Devis.class);
		assertNotNull(resultDevis);

		// verify that service method was called once
		verify(devisService).getEntityWithOptional(any(Long.class));

	}

	@Test
	@Order(4)
	@WithMockUser(username = "admin", password = "admin")
	public void test_updateDevis() throws Exception {
		
		
		when(devisService.getEntityWithOptional(any(Long.class))).thenReturn(liste.get(0));

		when(devisService.saveOrUpdateEntity(any(Devis.class), any(EtatOperation.class))).thenReturn(liste.get(1));

		mockMvc.perform(MockMvcRequestBuilders.put(getURL("liste/1"))
				.with(csrf()) // sinon 403
			    .content(objectMapper.writeValueAsString(liste.get(0)))
			    .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON))
			    .andExpect(status().isOk())
			    .andExpect(jsonPath("$.reference").value("1"))
			    .andReturn();
		
		// verify that service method was called once
		verify(devisService).saveOrUpdateEntity(any(Devis.class), any(EtatOperation.class));
	}

	@Test
	@Order(5)
	@WithMockUser(username = "admin", password = "admin")
	public void test_deleteDevis() throws Exception {
		
		mockMvc.perform(MockMvcRequestBuilders.delete(getURL("liste/1"))
			.with(csrf())) // sinon 403
			.andExpect(status().isGone());	
		
		// verify that service method was called once
		verify(devisService).deleteEntity(any(Long.class));
	}
	
	/**
	 * @return Devis
	 */
	public static Devis genererDevis() {
		boolean tva20Pourcent = ThreadLocalRandom.current().nextBoolean();
		double montantDevis = ThreadLocalRandom.current().nextDouble(200, 10000);
		double montantApport = ThreadLocalRandom.current().nextDouble(0, montantDevis);
		int dureerTravaux = ThreadLocalRandom.current().nextInt((int) Math.round(montantDevis/200));
		
		Devis devis = new Devis(
				LocalDate.now(), 
				montantDevis,
				tva20Pourcent ? 20.0 : 5.5,
				montantApport,
				ThreadLocalRandom.current().nextBoolean(),
				dureerTravaux);	
		
		return devis;
		
	}
	
	
}