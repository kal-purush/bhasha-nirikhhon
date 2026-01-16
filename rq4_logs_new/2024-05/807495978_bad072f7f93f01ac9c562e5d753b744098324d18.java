package se.lexicon.g49jpaworkshop;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import se.lexicon.g49jpaworkshop.entity.AppUser;
import se.lexicon.g49jpaworkshop.entity.Details;
import se.lexicon.g49jpaworkshop.repository.AppUserRepository;
import se.lexicon.g49jpaworkshop.repository.DetailsRepository;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@DataJpaTest
public class AppUserRepositoryTest {
    @Autowired
    AppUserRepository appUserRepository;

    @Autowired
    DetailsRepository detailsRepository;

    @Test
    public void saveAndFindByUserName() {
        AppUser appUser = new AppUser("john", "1234");

        //Save the appUser to the DB
        AppUser savedAppUser = appUserRepository.save(appUser);

        Optional<AppUser> retrievedAppUser = appUserRepository.findAppUserByUserName(savedAppUser.getUserName());

        // Assert that the saved AppUser is not null and has the expected values
        assertNotNull(retrievedAppUser);
        if (retrievedAppUser.isPresent()) {
            assertEquals(appUser.getUserName(), retrievedAppUser.get().getUserName());
            assertEquals(appUser.getPassword(), retrievedAppUser.get().getPassword());
        } else {
            fail("No AppUser found");
        }
        // Clean up the database
        appUserRepository.delete(savedAppUser);

    }

    @Test
    public void saveAndFindByDetailsEmail() {
        AppUser appUser = new AppUser("John", "Test");
        AppUser savedAppUser = appUserRepository.save(appUser);

        Details details = new Details("John Doe", "Test@test.com", LocalDate.now());
        Details savedDetails = detailsRepository.save(details);
        appUser.setUserDetails(savedDetails);

        Optional<AppUser> retrievedAppUser = appUserRepository.findAppUserByUserDetails_EmailIgnoreCase(savedDetails.getEmail());

        // Assert that the retrieved AppUser is not null and has the expected values
        assertTrue(retrievedAppUser.isPresent(), "No AppUser found");
        assertEquals(savedAppUser.getUserName(), retrievedAppUser.get().getUserName());
        assertEquals(savedAppUser.getPassword(), retrievedAppUser.get().getPassword());

        // Clean up the database
        detailsRepository.delete(savedDetails);
        appUserRepository.delete(savedAppUser);

    }

    @Test
public void findAppUserByRegDateBetween() {
    // Create new AppUsers
    AppUser appUser1 = new AppUser("John", "Test1");
    appUser1.setRegDate(LocalDate.now().minusDays(1));
    AppUser savedAppUser1 = appUserRepository.save(appUser1);

    AppUser appUser2 = new AppUser("Jane", "Test2");
    appUser2.setRegDate(LocalDate.now());
    AppUser savedAppUser2 = appUserRepository.save(appUser2);

    // Find AppUsers by registration date
    List<AppUser> appUsers = appUserRepository.findAppUserByRegDateBetween(LocalDate.now().minusDays(2), LocalDate.now());

    // Assert that the retrieved AppUsers are not null and have the expected values
    assertNotNull(appUsers);
    assertTrue(appUsers.contains(savedAppUser1));
    assertTrue(appUsers.contains(savedAppUser2));

    // Clean up the database
    appUserRepository.delete(savedAppUser1);
    appUserRepository.delete(savedAppUser2);
}

@Test
public void findAppUserByUserDetails_Id() {
    // Create new AppUser and UserDetails
    AppUser appUser = new AppUser("John", "Test");
    AppUser savedAppUser = appUserRepository.save(appUser);

    Details details = new Details("John Doe", "Test@test.com", LocalDate.now());
    Details savedDetails = detailsRepository.save(details);
    savedAppUser.setUserDetails(savedDetails);

    // Find AppUser by UserDetails id
    Optional<AppUser> retrievedAppUser = appUserRepository.findAppUserByUserDetails_Id(savedDetails.getId());

    // Assert that the retrieved AppUser is not null and has the expected values
    assertTrue(retrievedAppUser.isPresent(), "No AppUser found");
    assertEquals(savedAppUser.getUserName(), retrievedAppUser.get().getUserName());
    assertEquals(savedAppUser.getPassword(), retrievedAppUser.get().getPassword());

    // Clean up the database
    detailsRepository.delete(savedDetails);
    appUserRepository.delete(savedAppUser);
}

@Autowired
TestEntityManager entityManager;

@Test
public void updateUserNameById() {
    // Create a new AppUser
    AppUser appUser = new AppUser("John", "Test");
    AppUser savedAppUser = appUserRepository.save(appUser);

    // Update the username of the saved AppUser
    String newUserName = "Jane";
    int updatedRows = appUserRepository.updateUserNameById(newUserName, savedAppUser.getId());

    // Flush and clear the persistence context
    entityManager.flush();
    entityManager.clear();

    // Retrieve the updated AppUser
    Optional<AppUser> retrievedAppUser = appUserRepository.findById(savedAppUser.getId());

    // Assert that the username was updated
    assertEquals(1, updatedRows, "Expected one row to be updated");

    // Assert that the retrieved AppUser is not null and has the updated username
    assertTrue(retrievedAppUser.isPresent(), "No AppUser found");
    assertEquals(newUserName, retrievedAppUser.get().getUserName());

    // Clean up the database
    appUserRepository.delete(savedAppUser);
}
}