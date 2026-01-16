package pl.maks.carrental.validator;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import pl.maks.carrental.controller.productDTO.ClientDTO;
import pl.maks.carrental.exception.CarRentalValidationException;
import pl.maks.carrental.repository.ClientRepository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class ClientValidatorTest {

    private final ClientRepository clientRepository = mock(ClientRepository.class);
    private final ClientValidator target = new ClientValidator(clientRepository);

    @Test
    @DisplayName("Validation Error should not be thrown when input client is valid")
    void shouldNotThrow_whenClientIsValid() {
        //given
        ClientDTO valid = validClient();

        //when
        assertDoesNotThrow(() -> target.validateClient(valid));

        //then
    }

    @Test
    @DisplayName("Validation Error should be thrown when accidents count is null")
    void shouldThrow_whenAccidentsCountIsNull() {
        //given
        ClientDTO invalidAccidents = invalidAccidents();

        //when
        CarRentalValidationException carRentalValidationException = assertThrows(CarRentalValidationException.class, () -> target.validateClient(invalidAccidents));

        //then
        assertThat(carRentalValidationException.getViolations()).contains("Accidents count can't be null");
    }

    @Test
    @DisplayName("Validation Error should be thrown when LastName is invalid")
    void shouldThrow_whenLastLastNameIsInvalid() {
        //given
        ClientDTO invalidLastNameClient = invalidLastNameClient();
        String expectedMessage = (String.format("%s can contain only digits: '%s'", "LastName", invalidLastNameClient.getLastName()));

        //when
        CarRentalValidationException carRentalValidationException = assertThrows(CarRentalValidationException.class, () -> target.validateClient(invalidLastNameClient));

        //then
        assertThat(carRentalValidationException.getViolations().contains(expectedMessage));

    }

    @Test
    @DisplayName("Validation Error should be thrown when LastName is blank")
    void shouldThrow_whenLastNameIsBlank() {
        //given
        ClientDTO emptyLastNameClient = emptyLastNameClient();
        String expectedMessage = "LastName is blank";

        //when
        CarRentalValidationException carRentalValidationException = assertThrows(CarRentalValidationException.class, () -> target.validateClient(emptyLastNameClient));

        //then
        assertThat(carRentalValidationException.getViolations()).contains(expectedMessage);
    }

    @Test
    @DisplayName("Validation Error should be thrown when name is invalid")
    void shouldThrow_whenNameIsInvalid() {
        //given
        ClientDTO invalidNameClient = invalidNameClient();
        String expectedMessage = String.format("name can contain only letters: '%s'", invalidNameClient.getFirstName());

        //when
        CarRentalValidationException carRentalValidationException = assertThrows(CarRentalValidationException.class, () -> target.validateClient(invalidNameClient));

        //then
        assertThat(carRentalValidationException.getViolations().contains(expectedMessage));
    }

    @Test
    @DisplayName("Validation Error should be thrown when name is blank")
    void shouldThrow_whenNameIsBlank() {
        //given
        ClientDTO emptyNameClient = emptyNameClient();
        String expectedMessage = "firstName is blank";

        //when
        CarRentalValidationException carRentalValidationException = assertThrows(CarRentalValidationException.class, () -> target.validateClient(emptyNameClient));

        //then
        assertThat(carRentalValidationException.getViolations()).contains(expectedMessage);
    }

    private ClientDTO emptyNameClient() {
        ClientDTO dto = new ClientDTO();
        dto.setAccidents(11);
        dto.setDocumentNumber("dxz875848382");
        dto.setFirstName("");
        dto.setLastName("TestLastName");
        return dto;
    }

    private ClientDTO emptyLastNameClient() {
        ClientDTO dto = new ClientDTO();
        dto.setAccidents(11);
        dto.setDocumentNumber("dxz875848382");
        dto.setFirstName("TestFirstName");
        dto.setLastName("");
        return dto;

    }

    private ClientDTO validClient() {
        ClientDTO dto = new ClientDTO();
        dto.setAccidents(11);
        dto.setDocumentNumber("dxz875848382");
        dto.setFirstName("TestFirstName");
        dto.setLastName("TestLastName");
        return dto;
    }

    private ClientDTO invalidAccidents() {
        ClientDTO dto = new ClientDTO();
        dto.setDocumentNumber("dxz875848382");
        dto.setFirstName("TestFirstName");
        dto.setLastName("TestLastName");
        dto.setAccidents(null);
        return dto;
    }

    private ClientDTO invalidNameClient() {
        ClientDTO dto = new ClientDTO();
        dto.setDocumentNumber("dxz875848382");
        dto.setFirstName("TestFirstName");
        dto.setLastName("TestLastName");
        return dto;
    }

    private ClientDTO invalidLastNameClient() {
        ClientDTO dto = new ClientDTO();
        dto.setAccidents(11);
        dto.setDocumentNumber("dxz875848382");
        dto.setFirstName("TestFirstName");
        dto.setLastName("");
        return dto;
    }
}