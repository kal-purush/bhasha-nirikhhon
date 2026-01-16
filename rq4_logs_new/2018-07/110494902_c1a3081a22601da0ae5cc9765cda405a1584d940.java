package org.snobot.lib;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestInDeadbandHelper
{

    @Test
    public void testDeadbandHelper()
    {
        InDeadbandHelper helper = new InDeadbandHelper(10);

        for (int i = 0; i < 100; ++i)
        {
            Assertions.assertFalse(helper.isFinished((i % 2) == 0));
            Assertions.assertEquals((i % 2) == 0 ? 1 : 0, helper.inDeadband());
        }

        Assertions.assertFalse(helper.isFinished(false));

        for (int i = 0; i < 9; ++i)
        {
            Assertions.assertFalse(helper.isFinished(true));
            Assertions.assertEquals(i + 1, helper.inDeadband());
        }
        Assertions.assertTrue(helper.isFinished(true));
    }
}