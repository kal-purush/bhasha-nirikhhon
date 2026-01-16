package org.rinmakh;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.shell.Bootstrap;
import org.springframework.shell.CommandLine;
import org.springframework.shell.core.ExitShellRequest;
import org.springframework.shell.core.JLineShellComponent;
import org.springframework.stereotype.Component;

/**
 * @author Rinat_Makhmutov
 */
@Configuration
@ComponentScan(
        value ="com.rinmakh"
)
public class Application {

    private static ApplicationContext ctx;


    public static void main(String[] args) {
         ctx = new AnnotationConfigApplicationContext(Application.class);
        Application appl = ctx.getBean(Application.class);
        Bootstrap bootstrap = ctx.getBean(Bootstrap.class);
    }

    private ExitShellRequest runShell(){
        JLineShellComponent shell = ctx.getBean(JLineShellComponent.class);
        ExitShellRequest exitShellRequest;

        shell.start();
        shell.promptLoop();
        exitShellRequest = shell.getExitShellRequest();
        shell.waitForComplete();

        return  exitShellRequest;
    }

}