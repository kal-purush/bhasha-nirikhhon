package vkmbox.classifier;

import py4j.GatewayServer;
import lombok.extern.slf4j.Slf4j;
import vkmbox.classifier.ensemble.AdaBoostStandardClassifier;

/**
 *
 * @author vkmbox
 */
@Slf4j
public class Py4jStarter {
    public static void main(String[] args) {
        log.info("Starting py4j GatewayServer");
        AdaBoostStandardClassifier classifier = new AdaBoostStandardClassifier(150);
        // app is now the gateway.entry_point
        GatewayServer server = new GatewayServer(classifier);
        server.start();
        log.info("The instance of py4j GatewayServer is started");
    }
}