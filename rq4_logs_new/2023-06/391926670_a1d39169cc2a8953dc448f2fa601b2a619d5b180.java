package base;

import io.appium.java_client.service.local.AppiumDriverLocalService;
import io.appium.java_client.service.local.AppiumServiceBuilder;
import io.appium.java_client.service.local.flags.GeneralServerFlag;

import java.io.File;
import java.io.IOException;

import static io.appium.java_client.service.local.flags.GeneralServerFlag.*;

public class AppiumServerManager {

    private static AppiumDriverLocalService appiumLocalServer;
    private static int port = 4723;
    private static String url = "127.0.0.1";
    private static boolean serverIsRunning = false;

    public static AppiumDriverLocalService getAppiumLocalServer() {
        stopServer();
        if (!serverIsRunning) {
            startServer();
            serverIsRunning = appiumLocalServer.isRunning();
            System.out.println("*** Appium server started on: " + appiumLocalServer.getUrl());
        }
        System.out.println("*** Appium server is running: " + serverIsRunning);
        return appiumLocalServer;
    }

    public static void stopServer() {
        if (serverIsRunning) {
            appiumLocalServer.stop();
            System.out.println("*** Appium server stopped");
        }
        //appiumLocalServer.stop();
    }

    private static void startServer() {
        AppiumServiceBuilder builder = new AppiumServiceBuilder();
        builder.withIPAddress(url)
                .usingPort(port)
                //.usingAnyFreePort ()
                .withAppiumJS(
                        new File("C:\\Users\\QA\\AppData\\Roaming\\npm\\node_modules\\appium\\build\\lib\\main.js"))
                .usingDriverExecutable(new File("C:\\Program Files\\nodejs\\node.exe"))
                .withArgument(BASEPATH, "/wd/hub")
                //.withArgument(BASEPATH, "/")
                .withArgument(GeneralServerFlag.SESSION_OVERRIDE)
                // .withArgument(CONFIGURATION_FILE, "C:\\IdeaProjects\\CheckDriver\\pluginElementWait.json")
                .withArgument(() ->"--use-plugins", "element-wait")
                //.withArgument(SHOW_CONFIG)
                .withArgument(GeneralServerFlag.LOG_LEVEL, "debug");

        appiumLocalServer = AppiumDriverLocalService.buildService(builder);
        appiumLocalServer.start();
    }

    //Остановка серевера через командную строку
    public static void stopServerCMD() {
        Runtime runtime = Runtime.getRuntime();
        try {
            runtime.exec("taskkill /F /IM node.exe");
            runtime.exec("taskkill /F /IM cmd.exe");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}