package unit.test.LanguageDAO;

import java.util.ArrayList;
import mx.fei.coilvicapp.logic.language.*;
import mx.fei.coilvicapp.logic.implementations.DAOException;
import static org.junit.Assert.assertEquals;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.After;

public class LanguageTest {

    private static final Language TEST_LANGUAGE = new Language();
    private static final LanguageDAO LANGUAGE_DAO = new LanguageDAO();

    @Before
    public void setUp() throws DAOException {
        TEST_LANGUAGE.setName("Espaniol");
        int idTestLanguage = LANGUAGE_DAO.registerLanguage(TEST_LANGUAGE);
        TEST_LANGUAGE.setIdLanguage(idTestLanguage);
    }

    @After
    public void tearDown() throws DAOException {
        LANGUAGE_DAO.deleteLanguage(TEST_LANGUAGE.getIdLanguage());
    }

    @Test
    public void testSuccessInsertLanguage() throws DAOException {
        Language language = new Language();
        language.setName("Ingles");
        int idTestLanguage = LANGUAGE_DAO.registerLanguage(language);
        LANGUAGE_DAO.deleteLanguage(idTestLanguage);
        Assert.assertTrue(idTestLanguage > 0);
    }

    @Test(expected = DAOException.class)
    public void testFailureInsertLanguageByNameAlreadyRegistered() throws DAOException {
        Language language = new Language();
        language.setName(TEST_LANGUAGE.getName());
        LANGUAGE_DAO.registerLanguage(language);
    }

    @Test
    public void testSuccesUpdateLanguage() throws DAOException {
        Language language = new Language();
        language.setName("Francés");
        language.setIdLanguage(TEST_LANGUAGE.getIdLanguage());
        int result = LANGUAGE_DAO.updateLanguage(language);
        Assert.assertTrue(result > 0);
    }

    @Test(expected = DAOException.class)
    public void testFailureUpdateLanguageByAlreadyRegisteredName() throws DAOException {
        Language language = new Language();
        language.setName("Francés");
        language.setIdLanguage(TEST_LANGUAGE.getIdLanguage());

        Language auxLanguage = new Language();
        auxLanguage.setName("Francés");
        int idTestLanguage = LANGUAGE_DAO.registerLanguage(auxLanguage);
        try {
            LANGUAGE_DAO.updateLanguage(language);
        } finally {
            LANGUAGE_DAO.deleteLanguage(idTestLanguage);
        }
    }

    @Test
    public void testSuccessDeleteLanguage() throws DAOException {
        int result = LANGUAGE_DAO.deleteLanguage(TEST_LANGUAGE.getIdLanguage());
        Assert.assertTrue(result > 0);
    }

    @Test
    public void testFailureDeleteLanguageByIdNotAvailable() throws DAOException {
        int result = LANGUAGE_DAO.deleteLanguage(999);
        Assert.assertTrue(result == 0);
    }

    @Test
    public void testSuccessGetLanguageByName() throws DAOException {
        Language language = LANGUAGE_DAO.getLanguageByName(TEST_LANGUAGE.getName());
        Assert.assertEquals(TEST_LANGUAGE, language);
    }

    @Test
    public void testFailureGetLanguageByNameNotAvailable() throws DAOException {
        Language language = LANGUAGE_DAO.getLanguageByName("Alemán");
        Assert.assertNotEquals(TEST_LANGUAGE, language);
    }

    @Test
    public void testSuccessGetLanguageById() throws DAOException {
        Language language = LANGUAGE_DAO.getLanguageByIdLanguage(TEST_LANGUAGE.getIdLanguage());
        Assert.assertEquals(TEST_LANGUAGE, language);
    }

    @Test
    public void testFailureGetLanguageByIdNotAvailable() throws DAOException {
        Language language = LANGUAGE_DAO.getLanguageByIdLanguage(9999);
        Assert.assertNotEquals(TEST_LANGUAGE, language);
    }

    @Test
    public void testGetLanguagesSuccess() throws DAOException {
        ArrayList<Language> expectedLanguages = initializeLanguagesArray();
        ArrayList<Language> actualLanguages = LANGUAGE_DAO.getLanguages();
        assertEquals(expectedLanguages, actualLanguages);
        tearDownLanguagesArray(actualLanguages);
    }
    
    @Test
    public void testGetLanguagesFailure() throws DAOException {
        ArrayList<Language> expectedLanguages = initializeLanguagesArray();
        ArrayList<Language> actualLanguages = LANGUAGE_DAO.getLanguages();
        assertEquals(expectedLanguages, actualLanguages);
        tearDownLanguagesArray(actualLanguages);
    }    

    public ArrayList<Language> initializeLanguagesArray() throws DAOException {
        ArrayList<Language> languages = new ArrayList<>();
        languages.add(TEST_LANGUAGE);

        Language languageAux1 = new Language();
        languageAux1.setName("Alemán");

        Language languageAux2 = new Language();
        languageAux2.setName("Portugués");

        int idLanguage1 = LANGUAGE_DAO.registerLanguage(languageAux1);
        languageAux1.setIdLanguage(idLanguage1);
        languages.add(languageAux1);

        int idLanguage2 = LANGUAGE_DAO.registerLanguage(languageAux2);
        languageAux2.setIdLanguage(idLanguage2);
        languages.add(languageAux2);

        return languages;
    }

    private void tearDownLanguagesArray(ArrayList<Language> languages) throws DAOException {
        for (int i = 1; i < languages.size(); i++) {
            LANGUAGE_DAO.deleteLanguage(languages.get(i).getIdLanguage());
        }
    }
}