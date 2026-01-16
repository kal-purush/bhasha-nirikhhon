package sysjm3.bulbo.bulbo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import sysjm3.bulbo.bulbo.model.Card;
import sysjm3.bulbo.bulbo.model.Workspace;
import sysjm3.bulbo.bulbo.service.CardService;
import sysjm3.bulbo.bulbo.service.WorkspaceService;

/**
 *
 * @author Christoffer Hansen
 */
@Component
public class PreDatabaseFiller implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(PreDatabaseFiller.class);

    @Autowired
    private WorkspaceService workspaceService;

    @Autowired
    private CardService cardService;

    @Override
    public void run(String... args) throws Exception {
        // Create 2 workspaces and save them
        Workspace ws1 = new Workspace("_Workspace1");
        Workspace ws2 = new Workspace("_Workspace2");
        workspaceService.saveWorkspace(ws1);
        workspaceService.saveWorkspace(ws2);

        // Create 5 Cards and save them
        cardService.saveCard(new Card("_Card1", "_Card1-content", ws1));
        cardService.saveCard(new Card("_Card2", "_Card2-content", ws1));
        cardService.saveCard(new Card("_Card3", "_Card3-content", ws1));
        cardService.saveCard(new Card("_Card4", "_Card4-content", ws2));
        cardService.saveCard(new Card("_Card5", "_Card5-content", ws2));
        
        // Try to find any and all workspaces and cards and log them
        workspaceService.findAll().forEach((workspace) -> {
            logger.info("{}", workspace);
        });
        
        cardService.findAll().forEach((card) -> {
            logger.info("{}", card);
        });
        
    }

}