package pl.maks.carrental.validator;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import pl.maks.carrental.controller.productDTO.CarDTO;
import pl.maks.carrental.controller.productDTO.ClientDTO;
import pl.maks.carrental.controller.productDTO.ContractDTO;
import pl.maks.carrental.exception.CarRentalValidationException;
import pl.maks.carrental.repository.ContractRepository;

import java.math.BigDecimal;
import java.sql.Date;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class ContractValidatorTest {
    private final ContractRepository contractRepository = mock(ContractRepository.class);
    private final ContractValidator target = new ContractValidator(contractRepository);
    protected final CarDTO carDTO = mock(CarDTO.class);
    private final ClientDTO clientDTO = mock(ClientDTO.class);

    @Test
    @DisplayName("Validation Error should not be thrown when input contract is valid")
    void shouldNotThrow_whenContractIsValid() {
        // Given
        ContractDTO valid = validContract();

        // When, Then
        assertDoesNotThrow(() -> target.validateContract(valid));
    }

    @Test
    @DisplayName("Validation Error should be thrown when car ID is null")
    void shouldThrow_whenCarIdIsNull() {
        // Given
        ContractDTO carIdNull = CarIdNull();
        carIdNull.getCar().setId(null);

        // When, Then
        assertThrows(CarRentalValidationException.class, () -> target.validateContract(carIdNull));
    }

    @Test
    @DisplayName("Validation Error should be thrown when client ID is null")
    void shouldThrow_whenClientIdIsNull() {
        // Given
        ContractDTO clientIdNull = ClientIdNull();
        clientIdNull.getClient().setId(null);

        // When, Then
        assertThrows(CarRentalValidationException.class, () -> target.validateContract(clientIdNull));
    }

    @Test
    @DisplayName("Validation Error should be thrown when start date is null")
    void shouldThrow_whenStartDateIsNull() {
        // Given
        ContractDTO dateStart = DateStart();
        dateStart.setStartDate(null);

        // When, Then
        assertThrows(CarRentalValidationException.class, () -> target.validateContract(dateStart));
    }

    @Test
    @DisplayName("Validation Error should be thrown when end date is null")
    void shouldThrow_whenEndDateIsNull() {
        // Given
        ContractDTO endDate = EndDate();
        endDate.setEndDate(null);

        // When, Then
        assertThrows(CarRentalValidationException.class, () -> target.validateContract(endDate));
    }

    @Test
    @DisplayName("Validation Error should be thrown when price is null")
    void shouldThrow_whenPriceIsNull() {
        // Given
        ContractDTO priceIsNull = PriceIsNull();
        priceIsNull.setPrice(null);

        // When, Then
        assertThrows(CarRentalValidationException.class, () -> target.validateContract(priceIsNull));
    }

    @Test
    @DisplayName("Validation Error should be thrown when price is less than or equal to zero")
    void shouldThrow_whenPriceIsLessThanOrEqualToZero() {
        // Given
        ContractDTO priceEqualToZero = PriceEqualToZero();
        priceEqualToZero.setPrice(BigDecimal.valueOf(-10.0));

        // When, Then
        assertThrows(CarRentalValidationException.class, () -> target.validateContract(priceEqualToZero));
    }
    private ContractDTO validContract() {
        ContractDTO dto = new ContractDTO();
        dto.setCar(carDTO);
        dto.setClient(clientDTO);
        dto.setStartDate(Date.valueOf("2023-07-15"));
        dto.setEndDate(Date.valueOf("2023-07-16"));
        dto.setPrice(new BigDecimal(150.0));
        return dto;
    }

    private ContractDTO CarIdNull() {
        ContractDTO dto = new ContractDTO();
        dto.setCar(carDTO);
        dto.setClient(clientDTO);
        dto.setStartDate(Date.valueOf("2023-07-15"));
        dto.setEndDate(Date.valueOf("2023-07-16"));
        dto.setPrice(new BigDecimal(150.0));
        return dto;
    }

    private ContractDTO ClientIdNull() {
        ContractDTO dto = new ContractDTO();
        dto.setCar(carDTO);
        dto.setClient(clientDTO);
        dto.setStartDate(Date.valueOf("2023-07-15"));
        dto.setEndDate(Date.valueOf("2023-07-16"));
        dto.setPrice(new BigDecimal(150.0));
        return dto;
    }

    private ContractDTO DateStart() {
        ContractDTO dto = new ContractDTO();
        dto.setCar(carDTO);
        dto.setClient(clientDTO);
        dto.setStartDate(Date.valueOf("2023-07-15"));
        dto.setEndDate(Date.valueOf("2023-07-16"));
        dto.setPrice(new BigDecimal(150.0));
        return dto;
    }

    private ContractDTO EndDate() {
        ContractDTO dto = new ContractDTO();
        dto.setCar(carDTO);
        dto.setClient(clientDTO);
        dto.setStartDate(Date.valueOf("2023-07-15"));
        dto.setEndDate(Date.valueOf("2023-07-16"));
        dto.setPrice(new BigDecimal(150.0));
        return dto;
    }

    private ContractDTO PriceIsNull() {
        ContractDTO dto = new ContractDTO();
        dto.setCar(carDTO);
        dto.setClient(clientDTO);
        dto.setStartDate(Date.valueOf("2023-07-15"));
        dto.setEndDate(Date.valueOf("2023-07-16"));
        dto.setPrice(new BigDecimal(0));
        return dto;
    }
    private ContractDTO PriceEqualToZero() {
        ContractDTO dto = new ContractDTO();
        dto.setCar(carDTO);
        dto.setClient(clientDTO);
        dto.setStartDate(Date.valueOf("2023-07-15"));
        dto.setEndDate(Date.valueOf("2023-07-16"));
        dto.setPrice(new BigDecimal(-10.0));
        return dto;
    }
}