package com.spencerk.Prompt;

import com.github.spencerk.Prompt.Prompt;
import com.github.spencerk.Prompt.PromptFactory;
import com.github.spencerk.UserData.UserData;
import org.junit.jupiter.api.*;

import java.io.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class GamePrompt {

    InputStream OG_IN;
    OutputStream OG_OUT;
    ByteArrayOutputStream TEST_OUT    = new ByteArrayOutputStream();

    //Change all input and output streams to test ones for IO testing
    @BeforeAll
    public void setUpTestIO() {
        OG_IN = System.in;
        OG_OUT = System.out;

        System.setOut(new PrintStream(TEST_OUT));
    }

    @BeforeEach
    public void clearStreams() {
        TEST_OUT.reset();

    }

    //Restore the streams to work on the CLI again
    @AfterAll
    public void restoreIO() {
        System.setIn(OG_IN);
        System.setOut(new PrintStream(OG_OUT));
    }

    @Test
    public void enterWord() {
        System.setIn(new ByteArrayInputStream("six\n6\n6\n6\n6\n6\n6".getBytes()));
        PromptFactory.getGamePrompt().run();

        //Strings at indeces - 0: game prompt, 1: Enter a guess (gets "six") 2: Enter a guess (Expected out put)
        assertEquals("Enter a guess inclusively between 1 and 20: ", TEST_OUT.toString().split("\n")[2]);
    }

    @Test
    public void enterBadNum() {
        String  lowResponse     = "Your guess is too low",
                highResponse    = "Your guess is too high",
                winResponse     = "You're right!",
                errorResponse   = null,
                goodResponse    = null;

        System.setIn(new ByteArrayInputStream("40\n6\n".getBytes()));
        //Setting a fail condition to end prompt execution early
        for(int i = 0; i < 6; i++) UserData.getInstance().incNumGuesses();
        PromptFactory.getGamePrompt().run();

        //Game prompt, ask and bad input, ask again. Index 2
        errorResponse = TEST_OUT.toString().split("\n")[2];

        //The same above list + response. Index 3
        goodResponse = TEST_OUT.toString().split("\n")[3];

        assertEquals("Enter a guess inclusively between 1 and 20: ", errorResponse);
        assertTrue(
                lowResponse.equals(goodResponse) ||
                         highResponse.equals(goodResponse) ||
                         winResponse.equals(goodResponse)
        );
    }

    @Test
    public void loseGame() {
        final String INPUT = "6\n6\n6\n6\n6\n6\n";
        String firstResponse = "";
        Prompt returnedPrompt = null;

        //Retry if 6 happens to be the right number. This test is about failing
        do {
            TEST_OUT.reset();
            System.setIn(new ByteArrayInputStream(INPUT.getBytes()));

            returnedPrompt = PromptFactory.getGamePrompt().run();

            //2 is the first response to an entered number
            firstResponse = TEST_OUT.toString().split("\n")[2];
        } while("You're right!".equals(firstResponse));

        assertTrue(returnedPrompt.getClass() == PromptFactory.getPlayAgainPrompt().getClass());
        //1 game prompt, 12 guess prompts, 1 better luck prompt. Index 14 needed
        assertEquals("Awww. Better luck next time!", TEST_OUT.toString().split("\n")[14]);
    }

}