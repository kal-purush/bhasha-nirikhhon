import com.tcs.edu.Message;
import com.tcs.edu.decorator.Severity;
import com.tcs.edu.service.CrudService;
import com.tcs.edu.service.CrudServiceImpl;
import com.tcs.edu.service.HashMapMessageRepository;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class HashMapTest {
     private CrudService messageRepository = new CrudServiceImpl(new HashMapMessageRepository());

    @Test
    public void saveElement(){
        Message message = new Message(Severity.MAJOR, "message");
        UUID id = messageRepository.post(message);

        assertTrue(messageRepository.getAll().contains(message));
        assertEquals(id,message.getId());
        assertEquals(message, messageRepository.getById(id));
    }

    @Test
    public void getAllElement(){
        Message message1 = new Message("message1");
        Message message2 = new Message(Severity.MAJOR, "message");
        Message message3 = new Message(Severity.MAJOR, "message");
        Message message4 = new Message(Severity.MAJOR, "message1");

        messageRepository.post(message1);
        messageRepository.post(message2);
        messageRepository.post(message3);

        assertTrue(messageRepository.getAll().contains(message1));
        assertTrue(messageRepository.getAll().contains(message2));
        assertTrue(messageRepository.getAll().contains(message3));
        assertFalse(messageRepository.getAll().contains(message4));

    }

    @Test
    public void deleteOneElement(){
        Message message1 = new Message("message1");
        Message message2 = new Message(Severity.MAJOR, "message");
        Message message3 = new Message(Severity.MAJOR, "message");

        UUID id = messageRepository.post(message1);
        messageRepository.post(message2);
        messageRepository.delete(id);

        assertTrue(messageRepository.getAll().contains(message2));
        assertFalse(messageRepository.getAll().contains(message1));
        IllegalArgumentException trow = assertThrows(IllegalArgumentException.class, () -> messageRepository.getById(id));
        assertEquals(String.format("Нет значения с id = %s", id), trow.getMessage());
    }

    @Test
    public void getElementById(){
        Message message1 = new Message("message1");
        Message message2 = new Message(Severity.MAJOR, "message");

        UUID id = messageRepository.post(message1);
        messageRepository.post(message2);

        assertEquals(message1,messageRepository.getById(id));
    }

    @Test
    public void getElementsByLevel(){
        Message message1 = new Message("message1");
        Message message2 = new Message(Severity.MAJOR, "message");
        Message message3 = new Message(Severity.MAJOR, "message");

        messageRepository.post(message1);
        messageRepository.post(message2);
        messageRepository.post(message3);

        Collection<Message> major;
        major = messageRepository.getBySeverity(Severity.MAJOR);
        assertTrue(major.contains(message2));
        assertTrue(major.contains(message3));
        assertFalse(major.contains(message1));
    }

    @Test
    public void updateElement(){
        Message message = new Message(Severity.MAJOR, "message");
        Message message1 = new Message(Severity.REGULAR, "message1");

        UUID id = messageRepository.post(message);
        message1.setId(id);
        messageRepository.update(message1);

        assertTrue(messageRepository.getAll().contains(message1));
        assertFalse(messageRepository.getAll().contains(message));
    }
}