package edu.java.bot;

import com.pengrad.telegrambot.model.Chat;
import com.pengrad.telegrambot.model.Message;
import com.pengrad.telegrambot.model.Update;
import edu.java.bot.comands.HelpCommand;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HelpCommandTest {
    private HelpCommand helpCommand;
    private Update update;

    @Before
    public void setUp() {
        helpCommand = new HelpCommand();

        update = mock(Update.class);
        Message message = mock(Message.class);
        Chat chat = mock(Chat.class);
        when(update.message()).thenReturn(message);
        when(message.text()).thenReturn("/help");
        when(message.chat()).thenReturn(chat);
    }

    @Test
    public void testCommand() {
        assertEquals("/help", helpCommand.command());
    }

    @Test
    public void testDescription() {
        assertEquals(" вывести окно с командами", helpCommand.description());
    }

    @Test
    public void testHandle() {
        String sendMessage = helpCommand.handle(update);

        assertNotNull(sendMessage);

        assertTrue(sendMessage.startsWith("Список доступных команд:"));
        assertTrue(sendMessage.contains("/start - зарегистрировать пользователя"));
        assertTrue(sendMessage.contains("/help -  вывести окно с командами"));
        assertTrue(sendMessage.contains("/track - начать отслеживание ссылки"));
        assertTrue(sendMessage.contains("/untrack - прекратить отслеживание ссылки"));
        assertTrue(sendMessage.contains("/list - показать список отслеживаемых ссылок"));
    }

    @Test
    public void testIsCorrect() {
        assertTrue(helpCommand.isCorrect(update));
    }
}