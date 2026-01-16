package de.elomagic.dttool.commands;

import de.elomagic.dttool.AbstractMockedServer;
import de.elomagic.dttool.App;
import de.elomagic.dttool.MockTool;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CollectBomsCommandTest extends AbstractMockedServer {

    @Test
    void testCollectAsZipFile() throws Throwable {
        MockTool.mockServer(getPort(), () -> {
            App app = new App();
            int exitCode = app.execute(new String[] { "collect-boms", "-v", "--filePattern=bom-%s1-%s2.json", "--file=./target/test/sboms.zip", "-pf=895425a3-6c1d-465a-9fda-6e21ea7a2035" });

            assertEquals(0, exitCode);
            assertTrue(Files.exists(Path.of("./target/test/sboms.zip")));
        });
    }

}