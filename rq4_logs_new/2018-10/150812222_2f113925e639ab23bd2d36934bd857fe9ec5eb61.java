package br.com.rbs.brewery.controller;

import br.com.rbs.brewery.domain.Beer;
import br.com.rbs.brewery.repository.BeerRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

public class BeerControllerTest {

    private MockMvc mockMvc;

    @Mock
    private BeerRepository beerRepositoryMocked;

    @InjectMocks
    private BeerController beerController;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders
                .standaloneSetup(beerController)
                .build();
    }

    @Test
    public void createOrUpdateBeer() throws Exception {
        Beer beer = new Beer("Beer 1", 1, 2);
        mockMvc.perform(
                post("/beer/beers")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(asJsonString(beer)))
                .andExpect(status().isOk());

        verify(beerRepositoryMocked, times(1)).save(Mockito.any());
    }

    @Test
    public void createOrUpdateBeerWithoutBeerStyle() throws Exception {
        Beer beer = new Beer();
        beer.setTemperatureMin(1);
        beer.setTemperatureMax(2);

        mockMvc.perform(
                post("/beer/beers")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(asJsonString(beer)))
                .andExpect(status().isBadRequest());
    }

    @Test
    public void createOrUpdateBeerWithoutTemperatureMin() throws Exception {
        Beer beer = new Beer();
        beer.setBeerStyle("Beer 1");
        beer.setTemperatureMax(2);

        mockMvc.perform(
                post("/beer/beers")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(asJsonString(beer)))
                .andExpect(status().isBadRequest());
    }

    @Test
    public void createOrUpdateBeerWithoutTemperatureMax() throws Exception {
        Beer beer = new Beer();
        beer.setBeerStyle("Beer 1");
        beer.setTemperatureMin(1);

        mockMvc.perform(
                post("/beer/beers")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(asJsonString(beer)))
                .andExpect(status().isBadRequest());
    }

    @Test
    public void createOrUpdateBeerList() throws Exception {
        List<Beer> beers = new ArrayList<>();
        beers.add(new Beer("Beer 1", 1, 2));
        beers.add(new Beer("Beer 2", 1, 2));
        beers.add(new Beer("Beer 3", 1, 2));

        mockMvc.perform(
                put("/beer/beers")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(asJsonString(beers)))
                .andExpect(status().isOk());

        verify(beerRepositoryMocked, times(3)).save(Mockito.any());
    }

    @Test
    public void deleteStudent() throws Exception {
        mockMvc.perform(
                delete("/beer/beers/{id}", 1)
                        .contentType(MediaType.APPLICATION_JSON)
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());

        verify(beerRepositoryMocked, times(1)).deleteById(Mockito.any());
    }

    @Test
    public void retrieveAllBeers() {
    }

    @Test
    public void retrieveBeer() {
    }

    @Test
    public void findPaginated() {
    }

    @Test
    public void retrieveBestBeer() {
        //TODO: Implementar comforme a montagem do algotritimo.
    }

    private static String asJsonString(final Object obj) {
        try {
            final ObjectMapper mapper = new ObjectMapper();
            final String jsonContent = mapper.writeValueAsString(obj);
            return jsonContent;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}