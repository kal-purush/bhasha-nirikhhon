import com.rh.Internship.services.Factory;
import com.rh.Internship.services.MessageService;
import org.junit.Test;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

/**
 * @author Saeed Zarinfam
 */
public class HelloMessageTest {

    @Test
    public void getMessageServiceDesktopFromFactory_getMessageForDesktopVersion_returnSpanishMessage(){
        MessageService messageService = Factory.getMessageServiceDesktop();

        assertThat(messageService.getHelloMessage(), equalTo("Bienvenida") );
    }

    @Test
    public void getMessageServiceFromFactory_getDefaultMessageVersion_returnEnglishMessage(){
        MessageService messageService = Factory.getMessageService();

        assertThat(messageService.getHelloMessage(), equalTo("Welcome") );
    }
}