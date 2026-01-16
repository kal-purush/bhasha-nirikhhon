package com.philderbeast.autopickup;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

import be.seeseemelk.mockbukkit.ServerMock;
import be.seeseemelk.mockbukkit.MockBukkit;

public class AutoPickupPluginTest {

    private ServerMock server;
    private AutoPickupPlugin plugin;

    @Before
    public void setUp()
    {
        server = MockBukkit.mock();
        plugin = (AutoPickupPlugin) MockBukkit.load((Class<AutoPickupPlugin>) AutoPickupPlugin.class);
    }

    @After
    public void tearDown()
    {
        MockBukkit.unload();
    }

    @Test
    public void dummyTest()
    {
        assertTrue(true);
    }
}