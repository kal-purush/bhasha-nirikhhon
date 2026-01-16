package com.github.jmcampanini.hrm.emulator;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;

public class Sandbox {

    @Test
    public void in_and_out() {
        List<String> instructions = new ArrayList<>();

        instructions.add("inbox");
        instructions.add("outbox");
    }

    @Test
    public void inbox_functionality() {
        Inbox inbox = mock(Inbox.class);

        try {
            // taking works because there's something on there
            String val = inbox.take();

        } catch (ProgramEndException e) {
            // seems like there's a checked exception for inbox being empty, triggering the END of a program
        }
    }

    @Test
    public void outbox_functionality() {
        Outbox outbox = mock(Outbox.class);
        String value = "test";
        outbox.put(value);
    }
}