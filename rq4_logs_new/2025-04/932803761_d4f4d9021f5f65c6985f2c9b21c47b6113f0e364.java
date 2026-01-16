package uk.ac.bangor.cs.cambria.AcademiGymraeg.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.security.authorization.AuthorizationDeniedException;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.ui.ConcurrentModel;
import org.springframework.ui.Model;
import org.springframework.validation.BeanPropertyBindingResult;
import org.springframework.validation.BindingResult;
import org.springframework.validation.ObjectError;

import uk.ac.bangor.cs.cambria.AcademiGymraeg.enums.Gender;
import uk.ac.bangor.cs.cambria.AcademiGymraeg.model.Noun;
import uk.ac.bangor.cs.cambria.AcademiGymraeg.repo.NounRepository;
import uk.ac.bangor.cs.cambria.AcademiGymraeg.util.NounService;
import uk.ac.bangor.cs.cambria.AcademiGymraeg.util.UserService;

@SpringBootTest
@ExtendWith(SpringExtension.class)
@ContextConfiguration
public class NounControllerUnitTests {

	@Autowired
	private NounController controller = new NounController();

	@Autowired
	private NounRepository nounRepo;

	@MockBean
	private UserService mockUserService;

	@MockBean
	private NounService mockNounService;

	private Noun testNounValid = new Noun() {
		{
			setNounId((long) 99);
			setEnglishNoun("afal");
			setWelshNoun("afal");
			setGender(Gender.MASCULINE);
		}
	};
	private Noun testNounInvalid = new Noun() {
		{
			setNounId((long) 999);
			setEnglishNoun("22");
			setWelshNoun("afal");
			setGender(Gender.MASCULINE);
		}
	};

	private Model testModel = new ConcurrentModel();
	private BindingResult brValid = new BeanPropertyBindingResult(testNounValid, "valid");
	private BindingResult brError = new BeanPropertyBindingResult(testNounInvalid, "error");

	/* Noun Admin Page Tests */

	// nounAdminPage - only works for Instructor
	@Test
	@WithMockUser(username = "student1@academigymraeg.com", roles = { "STUDENT", "ADMIN" })
	void nounAdminPage_OnlyInstructorAllowed() {

		AuthorizationDeniedException ex = assertThrows(AuthorizationDeniedException.class, () -> {
			controller.nounAdminPage(testModel);
		});

	}

	// nounAdminPage - userService returns null Id
	@Test
	@WithMockUser(username = "instructor@academigymraeg.com", roles = { "INSTRUCTOR" })
	void nounAdminPage_UserServiceReturnsNullId() {
		when(mockUserService.getLoggedInUserId()).thenReturn(null);
		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> {
			controller.nounAdminPage(testModel);
		});
		assertEquals(ex.getMessage(), "Unable to get logged in user");
	}

	// nounAdminPage - model already contains noun
	@Test
	@WithMockUser(username = "instructor@academigymraeg.com", roles = { "INSTRUCTOR" })
	void nounAdminPage_ModelAlreadyHasNoun_ExistingNounRemain() {
		Noun testNoun = new Noun();
		testNoun.setEnglishNoun("test");
		testModel.addAttribute("testNoun", testNoun);

		controller.nounAdminPage(testModel);

		Noun compareNoun = (Noun) testModel.getAttribute("testNoun");
		assertEquals(compareNoun.getEnglishNoun(), testNoun.getEnglishNoun());
	}

	// nounAdminPage - model does not contain noun
	@Test
	@WithMockUser(username = "instructor@academigymraeg.com", roles = { "INSTRUCTOR" })
	void nounAdminPage_ModelDoesNotHaveNoun_NewNounAdded() {
		assertNull(testModel.getAttribute("noun"));

		controller.nounAdminPage(testModel);

		Noun compareNoun = (Noun) testModel.getAttribute("noun");
		assertNotNull(compareNoun);
		assertNull(compareNoun.getNounId());
	}

	// nounAdminPage - correct confirm message is added to model (add)
	@Test
	@WithMockUser(username = "instructor@academigymraeg.com", roles = { "INSTRUCTOR" })
	void nounAdminPage_AddNounMessageNotNull_AddNounMessageAddedToModel() {
		controller.addConfirmationMessage = "ADD CONFIRM";

		controller.nounAdminPage(testModel);

		assertEquals("ADD CONFIRM", testModel.getAttribute("addconfirmationmessage"));
		assertEquals("", controller.addConfirmationMessage);
	}

	// nounAdminPage - correct confirm message is added to model (edit)
	@Test
	@WithMockUser(username = "instructor@academigymraeg.com", roles = { "INSTRUCTOR" })
	void nounAdminPage_EditNounMessageNotNull_EditNounMessageAddedToModel() {
		controller.editConfirmationMessage = "EDIT CONFIRM";

		controller.nounAdminPage(testModel);

		assertEquals("EDIT CONFIRM", testModel.getAttribute("editconfirmationmessage"));
		assertEquals("", controller.editConfirmationMessage);
	}

	// nounAdminPage - correct confirm message is added to model (delete)
	@Test
	@WithMockUser(username = "instructor@academigymraeg.com", roles = { "INSTRUCTOR" })
	void nounAdminPage_DeleteNounMessageNotNull_DeleteNounMessageAddedToModel() {
		controller.deleteConfirmationMessage = "DELETE CONFIRM";

		controller.nounAdminPage(testModel);

		assertEquals("DELETE CONFIRM", testModel.getAttribute("deleteconfirmationmessage"));
		assertEquals("", controller.deleteConfirmationMessage);
	}

	// nounAdminPage - all genders are added to model
	@Test
	@WithMockUser(username = "instructor@academigymraeg.com", roles = { "INSTRUCTOR" })
	void nounAdminPage_AllGenderENUMSAddedToModel() {
		List<Gender> allGenders = Arrays.asList(Gender.values());

		controller.nounAdminPage(testModel);

		assertEquals(allGenders, testModel.getAttribute("genders"));
	}

	// nounAdminPage - all nouns are added to model
	@Test
	@WithMockUser(username = "instructor@academigymraeg.com", roles = { "INSTRUCTOR" })
	void nounAdminPage_AllExistingNounsAddedToModel() {
		List<Noun> allNouns = new ArrayList<Noun>();

		Noun noun1 = new Noun();
		noun1.setNounId((long) 1);
		Noun noun2 = new Noun();
		noun2.setNounId((long) 2);
		allNouns.add(noun1);
		allNouns.add(noun2);

		when(mockNounService.getAllNouns()).thenReturn(allNouns);

		controller.nounAdminPage(testModel);

		assertEquals(allNouns, testModel.getAttribute("allnouns"));
	}

	// nounAdminPage - returns "nounadmin" string
	@Test
	@WithMockUser(username = "instructor@academigymraeg.com", roles = { "INSTRUCTOR" })
	void nounAdminPage_ReturnsCorrectString() {
		assertEquals(controller.nounAdminPage(testModel), "nounadmin");
	}

	/* newNoun tests */

	// newNoun - only works for Instructor
	@Test
	@WithMockUser(username = "student1@academigymraeg.com", roles = { "STUDENT", "ADMIN" })
	void newNoun_OnlyInstructorAllowed() {

		AuthorizationDeniedException ex = assertThrows(AuthorizationDeniedException.class, () -> {
			controller.newNoun(testNounValid, brValid, testModel);
		});

	}

	// newNoun - null Noun argument, throws error
	@Test
	@WithMockUser(username = "instructor@academigymraeg.com", roles = { "INSTRUCTOR" })
	void newNoun_NullNoun_ThrowsException() {
		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> {
			controller.newNoun(null, brValid, testModel);
		});
		assertEquals(ex.getMessage(), "Null Noun argument");
	}

	// newNoun - result has error - existing noun re-added to model
	@Test
	@WithMockUser(username = "instructor@academigymraeg.com", roles = { "INSTRUCTOR" })
	void newNoun_ResultHasError_ExistingNounReAdded() {
		assertNull(testModel.getAttribute("noun"));

		brError.addError(new ObjectError("noun", "noun error"));
		controller.newNoun(testNounInvalid, brError, testModel);

		Noun compareNoun = (Noun) testModel.getAttribute("noun");
		assertNotNull(compareNoun);
		assertEquals(compareNoun.getEnglishNoun(), testNounInvalid.getEnglishNoun());

	}

	// newNoun - noun is converted to lower case
	@Test
	@WithMockUser(username = "instructor@academigymraeg.com", roles = { "INSTRUCTOR" })
	void newNoun_ValidNounConvertedToLower() {
		testNounValid.setEnglishNoun("VALID");
		testNounValid.setWelshNoun("VALID");

		controller.newNoun(testNounValid, brValid, testModel);

		assertEquals(testNounValid.getEnglishNoun(), "valid");
		assertEquals(testNounValid.getWelshNoun(), "valid");

	}

	// newNoun - add confirmation message is updated
	@Test
	@WithMockUser(username = "instructor@academigymraeg.com", roles = { "INSTRUCTOR" })
	void newNoun_ValidNoun_AddConfirmationMessageUpdated() {
		controller.newNoun(testNounValid, brValid, testModel);

		assertEquals(controller.addConfirmationMessage,
				"New noun '" + testNounValid.getWelshNoun() + " | " + testNounValid.getEnglishNoun() + "' added.");

	}

	// newNoun - returns "redirect:/noun" string
	@Test
	@WithMockUser(username = "instructor@academigymraeg.com", roles = { "INSTRUCTOR" })
	void newNoun_ReturnsCorrectString() {
		assertEquals(controller.newNoun(testNounValid, brValid, testModel), "redirect:/noun");
	}

	/* deleteNoun tests */

	// deletenoun - only works for Instructor
	@Test
	@WithMockUser(username = "admin@academigymraeg.com", roles = { "STUDENT", "ADMIN" })
	void deletenoun_OnlyInstructorAllowed() {

		AuthorizationDeniedException ex = assertThrows(AuthorizationDeniedException.class, () -> {
			controller.deleteNoun((long) 9);
		});

	}

	// deleteNoun - null Noun id argument, throws error
	@Test
	@WithMockUser(username = "instructor@academigymraeg.com", roles = { "INSTRUCTOR" })
	void deleteNoun_NullNounId_ThrowsException() {
		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> {
			controller.deleteNoun(null);
		});
		assertEquals(ex.getMessage(), "Null id argument");
	}

	// deletenoun - unknown Noun id, throws error
	@Test
	@WithMockUser(username = "instructor@academigymraeg.com", roles = { "INSTRUCTOR" })
	void deletenoun_UnknownNounID_ThrowsException() {
		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> {
			controller.deleteNoun((long) -1);
		});
		assertEquals(ex.getMessage(), "Unknown Noun Id");
	}

	// deletenoun - delete confirmation message is updated
	@Test
	@WithMockUser(username = "instructor@academigymraeg.com", roles = { "INSTRUCTOR" })
	void deletenoun_KnownNoun_DeleteConfirmationMessageUpdated() {
		controller.newNoun(testNounValid, brValid, testModel);

		when(mockNounService.getById((long) 99)).thenReturn(Optional.of(testNounValid));

		controller.deleteNoun((long) 99);

		assertEquals(controller.deleteConfirmationMessage,
				"Noun '" + testNounValid.getWelshNoun() + " | " + testNounValid.getEnglishNoun() + "' was deleted.");

	}

	// deletenoun - returns "redirect:/noun" string
	@Test
	@WithMockUser(username = "instructor@academigymraeg.com", roles = { "INSTRUCTOR" })
	void deletenoun_ReturnsCorrectString() {
		when(mockNounService.getById((long) 99)).thenReturn(Optional.of(testNounValid));

		assertEquals(controller.deleteNoun((long) 99), "redirect:/noun");
	}

	/* editNoun tests */

	// editNoun - only works for Instructor
	@Test
	@WithMockUser(username = "student1@academigymraeg.com", roles = { "STUDENT", "ADMIN" })
	void editNoun_OnlyInstructorAllowed() {

		AuthorizationDeniedException ex = assertThrows(AuthorizationDeniedException.class, () -> {
			controller.editNoun(testNounValid, brValid, testModel);
		});

	}

	// editNoun - null Noun argument, throws error
	@Test
	@WithMockUser(username = "instructor@academigymraeg.com", roles = { "INSTRUCTOR" })
	void editNoun_NullNoun_ThrowsException() {
		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> {
			controller.editNoun(null, brValid, testModel);
		});
		assertEquals(ex.getMessage(), "Null Noun argument");
	}

	// editNoun - result has error - existing noun re-added to model
	@Test
	@WithMockUser(username = "instructor@academigymraeg.com", roles = { "INSTRUCTOR" })
	void editNoun_ResultHasError_ExistingNounReAdded() {
		when(mockNounService.getById((long) 999)).thenReturn(Optional.of(testNounInvalid));

		assertNull(testModel.getAttribute("noun"));

		brError.addError(new ObjectError("noun", "noun error"));
		controller.editNoun(testNounInvalid, brError, testModel);

		Noun compareNoun = (Noun) testModel.getAttribute("noun");
		assertNotNull(compareNoun);
		assertEquals(compareNoun.getEnglishNoun(), testNounInvalid.getEnglishNoun());

	}

	// editNoun - noun is converted to lower case
	@Test
	@WithMockUser(username = "instructor@academigymraeg.com", roles = { "INSTRUCTOR" })
	void editNoun_ValidNounConvertedToLower() {
		testNounValid.setEnglishNoun("VALID");
		testNounValid.setWelshNoun("VALID");

		controller.editNoun(testNounValid, brValid, testModel);

		assertEquals(testNounValid.getEnglishNoun(), "valid");
		assertEquals(testNounValid.getWelshNoun(), "valid");

	}

	// editNoun - edit confirmation message is updated
	@Test
	@WithMockUser(username = "instructor@academigymraeg.com", roles = { "INSTRUCTOR" })
	void editNoun_ValidNoun_EditConfirmationMessageUpdated() {
		controller.editNoun(testNounValid, brValid, testModel);

		assertEquals(controller.editConfirmationMessage, "Changes to noun were saved.");

	}

	// editNoun - returns "redirect:/noun" string
	@Test
	@WithMockUser(username = "instructor@academigymraeg.com", roles = { "INSTRUCTOR" })
	void editNoun_ReturnsCorrectString() {
		assertEquals(controller.editNoun(testNounValid, brValid, testModel), "redirect:/noun");
	}

	// editNoun - new Noun is added to model after valid noun is updated
	@Test
	@WithMockUser(username = "instructor@academigymraeg.com", roles = { "INSTRUCTOR" })
	void editNoun_ValidNounUpdated_NewNounAddedToModel() {

		controller.editNoun(testNounInvalid, brError, testModel);

		Noun compareNoun = (Noun) testModel.getAttribute("noun");
		assertNotNull(compareNoun);
		assertNotEquals(compareNoun.getEnglishNoun(), testNounInvalid.getEnglishNoun());

	}

	/* nounEditPage tests */

	// nounEditPage - only works for Instructor
	@Test
	@WithMockUser(username = "student1@academigymraeg.com", roles = { "STUDENT", "ADMIN" })
	void nounEditPage_OnlyInstructorAllowed() {

		AuthorizationDeniedException ex = assertThrows(AuthorizationDeniedException.class, () -> {
			controller.nounEditPage((long) 99, testModel);
		});

	}

	// nounEditPage - null Noun id, throws error
	@Test
	@WithMockUser(username = "instructor@academigymraeg.com", roles = { "INSTRUCTOR" })
	void nounEditPage_NullNounID_ThrowsException() {
		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> {
			controller.nounEditPage(null, testModel);
		});
		assertEquals(ex.getMessage(), "Null id argument");
	}

	// nounEditPage - unknown Noun id, throws error
	@Test
	@WithMockUser(username = "instructor@academigymraeg.com", roles = { "INSTRUCTOR" })
	void nounEditPage_UnknownNounID_ThrowsException() {
		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> {
			controller.nounEditPage((long) -1, testModel);
		});
		assertEquals(ex.getMessage(), "Unknown Noun Id");
	}

	// nounEditPage - known Noun id, Noun added to model
	@Test
	@WithMockUser(username = "instructor@academigymraeg.com", roles = { "INSTRUCTOR" })
	void nounEditPage_KnownNounID_NoExistingNounInModel_RetrievedNounAddedToModel() {
		when(mockNounService.getById((long) 99)).thenReturn(Optional.of(testNounValid));

		controller.nounEditPage((long) 99, testModel);

		Noun retrievedNoun = (Noun) testModel.getAttribute("noun");

		assertEquals(retrievedNoun, testNounValid);

	}

	@Test
	@WithMockUser(username = "instructor@academigymraeg.com", roles = { "INSTRUCTOR" })
	void nounEditPage_KnownNounID_ExistingNounInModel_ExistingNounRemainsInModel() {
		Noun existingNoun = new Noun() {
			{
				setNounId((long) 9999);
			}
		};

		testModel.addAttribute(existingNoun);

		when(mockNounService.getById((long) 99)).thenReturn(Optional.of(testNounValid));

		controller.nounEditPage((long) 99, testModel);

		Noun retrievedNoun = (Noun) testModel.getAttribute("noun");

		assertEquals(retrievedNoun, existingNoun);

	}

	// nounEditPage - all genders are added to model
	@Test
	@WithMockUser(username = "instructor@academigymraeg.com", roles = { "INSTRUCTOR" })
	void nounEditPage_AllGenderENUMSAddedToModel() {
		List<Gender> allGenders = Arrays.asList(Gender.values());

		when(mockNounService.getById((long) 99)).thenReturn(Optional.of(testNounValid));

		controller.nounEditPage((long) 99, testModel);

		assertEquals(allGenders, testModel.getAttribute("genders"));
	}

	// nounEditPage - returns "nounedit" string
	@Test
	@WithMockUser(username = "instructor@academigymraeg.com", roles = { "INSTRUCTOR" })
	void nounEditPage_ReturnsCorrectString() {
		when(mockNounService.getById((long) 99)).thenReturn(Optional.of(testNounValid));
		assertEquals(controller.nounEditPage((long) 99, testModel), "nounedit");
	}

}