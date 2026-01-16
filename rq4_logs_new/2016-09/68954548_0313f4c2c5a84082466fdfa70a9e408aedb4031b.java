import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by DaryaKolyadko on 26.07.2016.
 */
public class PropertiesManager {
    public static final String CHROME_DRIVER_PATH = "webdriver.chrome.driver";

    private static final String CONFIG_FILE_NAME = "path.properties";
    private static final String ERROR = "Application cannot run without config file: ";

    private static AtomicBoolean initialized = new AtomicBoolean(false);
    private static Lock managerSingleLock = new ReentrantLock();

    private static Properties configuration;
    private static PropertiesManager manager;

    private PropertiesManager() {
        configuration = new Properties();
        URL configFile = PropertiesManager.class.getClassLoader().getResource(CONFIG_FILE_NAME);

        if (configFile == null) {
            throw new RuntimeException(ERROR + CONFIG_FILE_NAME);
        }

        try (FileInputStream inputStream = new FileInputStream(configFile.getFile())) {
            configuration.load(inputStream);
        } catch (IOException e) {
            throw new RuntimeException(ERROR + CONFIG_FILE_NAME);
        }
    }

    public static PropertiesManager getInstance() {
        if (!initialized.get()) {
            managerSingleLock.lock();

            if (manager == null) {
                manager = new PropertiesManager();
                initialized.set(true);
            }

            managerSingleLock.unlock();
        }

        return manager;
    }

    public String getProperty(String key) {
        return configuration.getProperty(key);
    }
}