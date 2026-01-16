import com.endava.jbehave.UnauthPlayerSteps;
import net.serenitybdd.junit.runners.SerenityRunner;
import net.thucydides.core.annotations.Steps;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(SerenityRunner.class)
public class UnauthGetPlayerTest {

    @Steps
    UnauthPlayerSteps unauthPlayerSteps;

    @Test
    public void startTrack() {
        unauthPlayerSteps.an_uri_that_belongs_to_a_dios_le_pido_track("spotify:track:7HJKzIgaNgDgo3p3a2ToR6");
        unauthPlayerSteps.user_executes_player_api();
        unauthPlayerSteps.user_listens_a_dios_le_pido_track();
    }
}