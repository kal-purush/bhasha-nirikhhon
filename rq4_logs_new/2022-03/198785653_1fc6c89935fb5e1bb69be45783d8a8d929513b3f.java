package eu.aequos.gogas;

import eu.aequos.gogas.mock.MockOrders;
import eu.aequos.gogas.mock.MockUsers;
import eu.aequos.gogas.mvc.MockMvcGoGas;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;

@SuppressWarnings("squid:S2187")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootTest
@AutoConfigureMockMvc
public class BaseGoGasIntegrationTest {

    @Autowired
    protected MockMvcGoGas mockMvcGoGas;

    @Autowired
    protected MockUsers mockUsers;

    @Autowired
    protected MockOrders mockOrders;

    @BeforeAll
    void init() {
        mockUsers.init();
    }

    @AfterAll
    void destroy() {
        mockOrders.destroy();
        mockUsers.destroy();
    }
}