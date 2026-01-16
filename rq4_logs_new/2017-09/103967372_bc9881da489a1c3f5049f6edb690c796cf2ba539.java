package uk.co.staticvoid.gliderrider.helper;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import uk.co.staticvoid.gliderrider.GliderRider;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({GliderRider.class})
public class ConfigHelperWithExistingTest {

    private GliderRider plugin = mock(GliderRider.class);

    private static final String TEST_OUTPUT_FOLDER = "./src/test/output/";
    private static final String TEST_FILE_NAME = "checkpoint.yml";

    private ConfigHelper underTest = new ConfigHelper(plugin, TEST_FILE_NAME);

    @Before
    public void setup() throws IOException {
        Files.createFile(new File(TEST_OUTPUT_FOLDER, TEST_FILE_NAME).toPath());
        when(plugin.getDataFolder()).thenReturn(new File(TEST_OUTPUT_FOLDER));
    }

    @Test
    public void shouldNotSaveDefaultIfOnePresent() throws IOException {
        underTest.saveDefaultConfig();
        verify(plugin, times(0)).saveResource(TEST_FILE_NAME, false);
    }

    @After
    public void cleanUp() throws Exception{
        Files.deleteIfExists(new File(TEST_OUTPUT_FOLDER, TEST_FILE_NAME).toPath() );
    }
}