import java.util.logging.Logger;

public class MarsRoverApplication {

    private static Controller controller;
    private static Logger LOGGER = Logger.getLogger(MarsRoverApplication.class.getName());

    public static void main(String[] args){
        controller = new Controller();
        LOGGER.info(controller.runRovers());
    }
}