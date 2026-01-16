package login.app;

import login.controller.Controller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by mrivero on 25/3/18.
 */
public class App {

    public static Logger LOGGER = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        LOGGER.info("Starting services configuration");
        Controller.initInterface();
        LOGGER.info("Finish services configuration");
    }
}