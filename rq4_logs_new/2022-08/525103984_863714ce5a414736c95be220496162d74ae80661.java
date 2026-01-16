package servicesTests;

import daos.registry.RegistryDao;
import daos.registry.RegistryDaoPostgres;
import entities.Registry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import services.registry.RegistryServices;
import services.registry.RegistryServicesImpl;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RegistryServicesTests {

    private Object mock;
    RegistryDao registryDaoMock = mock(RegistryDaoPostgres.class);
    RegistryServices registryServices = new RegistryServicesImpl(registryDaoMock);
    Registry test = new Registry("john", "smith", "test", "test");

    @Test
    void registry_get_person_by_login_service_test_Standard(){
        String testLogin = "test";
        String testPass = "test";
        when(registryDaoMock.getPersonByLogin(testLogin, testPass)).thenReturn(test);
        Registry result = registryServices.getPersonByLogin(testLogin,testPass);

        Assertions.assertEquals("john", result.getFirstName());

    }
}