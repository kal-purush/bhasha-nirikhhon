package homework.lab2.client;

import homework.lab2.client.Client;
import org.junit.jupiter.api.Test;

import static homework.lab2.client.SalaryConstants.PROGRAMMER_SALARY;
import static org.junit.jupiter.api.Assertions.*;

class ClientTest {

    @Test
    void testClientCreation() {
        var clientBuilder = new Client.ClientBuilder();

        var clientContacts = new Contacts(new Name("FirstName", "LastName"));
        clientContacts.setPhoneNumber("+9999999999");
        Client client = clientBuilder.setContacts(clientContacts).setDebt(5).build();

        assertEquals(new Name("FirstName", "LastName"), client.getContacts().getName());
        assertEquals(5, client.getDebt());
        assertEquals(PROGRAMMER_SALARY, client.getSalary());
    }

    @Test
    void testContactsException() {
        var clientBuilder = new Client.ClientBuilder();

        assertThrows(IllegalArgumentException.class, clientBuilder::build);
    }
}