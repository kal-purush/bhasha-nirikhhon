package ua.com.dashboard.controllers.rate;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import ua.com.dashboard.services.rate.RateService;
import ua.com.dashboard.view.rate.Rate;

import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.doReturn;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@ExtendWith(MockitoExtension.class)
class RateControllerTest {

    @Mock
    RateService rateService;
    @InjectMocks
    RateController rateController;

    @Test
    @SneakyThrows
    void getAllRates() {

        List<Rate> rateList = Arrays.asList(
                new Rate(17, "978", "EUR", "євро", "38.4518", "-0.0322", "09.11.2023"),
                new Rate(53, "840", "USD", "Долар США", "36.0407", "0.0036", "09.11.2023")
        );

        doReturn(rateList).when(this.rateService).getAllRates(null, "21-11-2023");

        MockMvc mockMvc = MockMvcBuilders.standaloneSetup(this.rateController).build();

        mockMvc.perform(get("/dashboard/rates/all?date=2023-11-21"))
                .andExpect(status().isOk())
                .andExpect(view().name("dashboard"))
                .andExpect(model().attributeExists("rates"))
                .andExpect(model().attribute("rates", rateList));
    }
}