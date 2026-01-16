package se.lexicon.g49jpaworkshop;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import se.lexicon.g49jpaworkshop.entity.Details;
import se.lexicon.g49jpaworkshop.repository.DetailsRepository;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DataJpaTest
public class DetailsRepositoryTest {

    @Autowired
    private DetailsRepository detailsRepository;

    @Autowired
    private TestEntityManager entityManager;

    @Test
    public void updateDetailsById() {
        // Create a new Details
        Details details = new Details("John Doe", "Test@test.com", LocalDate.now());
        Details savedDetails = detailsRepository.save(details);

        // Update the details of the saved Details
        String newName = "Jane Doe";
        String newEmail = "Test2@test.com";
        LocalDate newBirthday = LocalDate.now().minusYears(1);
        int updatedRows = detailsRepository.updateDetailsById(newName, newEmail, newBirthday, savedDetails.getId());

        // Flush and clear the persistence context
        entityManager.flush();
        entityManager.clear();

        // Retrieve the updated Details
        Optional<Details> retrievedDetails = detailsRepository.findById(savedDetails.getId());

        // Assert that the details were updated
        assertEquals(1, updatedRows, "Expected one row to be updated");

        // Assert that the retrieved Details is not null and has the updated details
        assertTrue(retrievedDetails.isPresent(), "No Details found");
        assertEquals(newName, retrievedDetails.get().getName());
        assertEquals(newEmail, retrievedDetails.get().getEmail());
        assertEquals(newBirthday, retrievedDetails.get().getBirthDay());

        // Clean up the database
        detailsRepository.delete(savedDetails);
    }

@Test
public void findDetailsByNameContains() {
    // Create a new Details
    Details details = new Details("John Doe", "Test@test.com", LocalDate.now());
    Details savedDetails = detailsRepository.save(details);

    // Retrieve the Details by name
    Optional<Details> retrievedDetails = detailsRepository.findDetailsByNameContains(savedDetails.getName().substring(0, 4));

    // Assert that the retrieved Details is not null and has the expected values
    assertTrue(retrievedDetails.isPresent(), "No Details found");
    assertEquals(savedDetails.getName(), retrievedDetails.get().getName());

    // Clean up the database
    detailsRepository.delete(savedDetails);
}

@Test
public void findDetailsByNameContainsIgnoreCase() {
    // Create a new Details
    Details details = new Details("John Doe", "Test@test.com", LocalDate.now());
    Details savedDetails = detailsRepository.save(details);

    // Retrieve the Details by name
    Optional<Details> retrievedDetails = detailsRepository.findDetailsByNameContainsIgnoreCase(savedDetails.getName().substring(0, 4).toUpperCase());

    // Assert that the retrieved Details is not null and has the expected values
    assertTrue(retrievedDetails.isPresent(), "No Details found");
    assertEquals(savedDetails.getName(), retrievedDetails.get().getName());

    // Clean up the database
    detailsRepository.delete(savedDetails);
}

    @Test
    public void findDetailsByEmailIgnoreCase() {
        // Create a new Details
        Details details = new Details("John Doe", "Test@test.com", LocalDate.now());
        Details savedDetails = detailsRepository.save(details);

        // Retrieve the Details by email
        Optional<Details> retrievedDetails = detailsRepository.findDetailsByEmailIgnoreCase(savedDetails.getEmail().toUpperCase());

        // Assert that the retrieved Details is not null and has the expected values
        assertTrue(retrievedDetails.isPresent(), "No Details found");
        assertEquals(savedDetails.getName(), retrievedDetails.get().getName());
        assertEquals(savedDetails.getEmail(), retrievedDetails.get().getEmail());
        assertEquals(savedDetails.getBirthDay(), retrievedDetails.get().getBirthDay());

        // Clean up the database
        detailsRepository.delete(savedDetails);
    }
}