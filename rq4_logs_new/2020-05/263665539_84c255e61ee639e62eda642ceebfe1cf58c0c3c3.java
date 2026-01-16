package sysjm3.bulbo.bulbo;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import sysjm3.bulbo.bulbo.model.Card;
import sysjm3.bulbo.bulbo.model.Workspace;
import sysjm3.bulbo.bulbo.service.CardService;
import sysjm3.bulbo.bulbo.service.WorkspaceService;

/**
 * Unit tests for the Workspace and Card repository
 * 
 * @author Christoffer Hansen
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class RepositoryUnitTests {

    private final Workspace TEST_WORKSPACE = new Workspace("TEST_WORKSPACE");
    private final Card TEST_CARD = new Card("TEST_CARD", "TEST_CARD_CONTENT",
            TEST_WORKSPACE);

    @Autowired
    private WorkspaceService workspaceService;

    @Autowired
    private CardService cardService;

    private void createTestSubjects() {
        workspaceService.saveWorkspace(TEST_WORKSPACE);
        cardService.saveCard(TEST_CARD);
    }

    /**
     * Assert by False that the WORKSPACES table is empty
     */
    @Test
    public void TestWorkspacesExist() {
        assertFalse(workspaceService.findAll().isEmpty());
    }

    /**
     * Assert by False that the CARDS table is empty
     */
    @Test
    public void TestCardsExist() {
        assertFalse(cardService.findAll().isEmpty());
    }

    /**
     * Assert by False that the WORKSPACES table size changes after trying to
     * add a Workspace
     */
    @Test
    public void TestCheckIfWorkspaceIsAdded() {
        int preSaveSize = workspaceService.findAll().size();
        createTestSubjects();
        assertFalse(workspaceService.findAll().size() == preSaveSize);
    }

    /**
     * Assert by True that the Workspace with given UUID exists
     */
    @Test
    public void TestCheckFindWorkspaceByUUID() {
        createTestSubjects();
        assertTrue(workspaceService.findOne(TEST_WORKSPACE.getUUID()) != null);
    }

    /**
     * Assert by False that the CARDS table size changes after trying to add a
     * Card
     */
    @Test
    public void TestCheckIfCardIsAdded() {
        int preSaveSize = cardService.findAll().size();
        createTestSubjects();
        assertFalse(cardService.findAll().size() == preSaveSize);
    }

    /**
     * Assert by True that the Card with given UUID exists
     */
    @Test
    public void TestCheckFindCardByUUID() {
        createTestSubjects();
        assertTrue(cardService.findOne(TEST_CARD.getUUID()) != null);
    }

    /**
     * Assert by False that the returned Collection of WORKSPACES with given
     * UUID is empty
     */
    @Test
    public void TestCheckFindCardsByWorkspace() {
        createTestSubjects();
        assertFalse(cardService.findByWorkspace(TEST_WORKSPACE.getUUID()).isEmpty());
    }

    /**
     * Assert by True that the Card with given UUID gets deleted
     */
    @Test
    public void TestCheckIfCardIsDeleted() {
        createTestSubjects();
        cardService.deleteCardById(TEST_CARD.getUUID());
        assertTrue(cardService.findOne(TEST_CARD.getUUID()) == null);
    }

    /**
     * Assert by True that the Workspace with given UUID gets deleted
     */
    @Test
    public void TestCheckIfWorkspaceIsDeleted() {
        createTestSubjects();
        int preDeleteSize = workspaceService.findAll().size();
        workspaceService.deleteWorkspaceById(TEST_WORKSPACE.getUUID());
        assertFalse(workspaceService.findAll().size() == preDeleteSize);
    }

}