package com.github.hcsp.controlflow;

import static org.hamcrest.Matchers.containsStringIgnoringCase;

import com.github.blindpirate.extensions.CaptureSystemOutput;
import org.junit.jupiter.api.Test;

public class MainTest {
    @Test
    @CaptureSystemOutput
    public void importStringUtilsCorrectly(CaptureSystemOutput.OutputCapture capture) {
        capture.expect(containsStringIgnoringCase("153"));
        capture.expect(containsStringIgnoringCase("370"));
        capture.expect(containsStringIgnoringCase("371"));
        capture.expect(containsStringIgnoringCase("407"));
        Main.printNarcissisticNumber();
    }
}