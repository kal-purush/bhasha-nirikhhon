package org.wso2.rule.validator.functions.core;

import org.junit.jupiter.api.Test;
import org.wso2.rule.validator.InvalidRulesetException;
import org.wso2.rule.validator.document.LintTarget;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class FalsyFunctionTest {
    private final FalsyFunction falsy = new FalsyFunction(null);

    @Test
    void givenFalsyInputsShouldReturnNoErrorMessage() {
        List<Object> falsyInputs = new ArrayList<>(List.of(false, null, 0, ""));

        for (Object input : falsyInputs) {
            LintTarget target = new LintTarget(new ArrayList<>(), input);
            try {
                boolean result = falsy.execute(target);
                assertTrue(result, "Expected falsy input: " + input + " to be falsy");
            } catch (InvalidRulesetException e) {
                fail("Exception thrown for falsy input: " + e.getMessage());
            }
        }
    }

    @Test
    void givenTruthyInputsShouldReturnErrorMessage() {
        List<Object> truthyInputs = List.of(true, 1, new ArrayList<>(List.of(1)),
                new HashMap<>(Map.of("key", "value")));

        for (Object input : truthyInputs) {
            LintTarget target = new LintTarget(new ArrayList<>(), input);
            try {
                boolean result = falsy.execute(target);
                assertFalse(result, "Expected truthy input: " + input + " to be truthy");
            } catch (InvalidRulesetException e) {
                fail("Exception thrown for truthy input: " + e.getMessage());
            }
        }
    }

    @Test
    void validationTestForInvalidOptions() {
        Map<String, Object> invalidOption = Map.of("unsupportedKey", true); // Unsupported key

        FalsyFunction function = new FalsyFunction(invalidOption);
        LintTarget target = new LintTarget(new ArrayList<>(), null);

        assertThrows(InvalidRulesetException.class, () -> function.execute(target),
                "Expected InvalidRulesetException for invalid option: " + invalidOption);
    }
}