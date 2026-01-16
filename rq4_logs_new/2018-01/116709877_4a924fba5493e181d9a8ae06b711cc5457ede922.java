package pl.kostrowski.nauka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.kostrowski.nauka.timecalculator.MethodChooser;

import java.util.Scanner;


public class App {
    public static void main(String[] args) {

        Logger LOG = LoggerFactory.getLogger(App.class);

        String option = new String();
        Scanner sc = new Scanner(System.in);
        MethodChooser mc = new MethodChooser();
        LOG.info("Witaj w kalkulatorze dat i czasu");


        do {
            LOG.info("Aby uzyskać różnicę między datami wpisz \"1\"");
            LOG.info("Aby uzyskać różnicę między godzinami wpisz \"2\"");
            LOG.info("Aby wyjść wpisz \"x\"");

            option = sc.nextLine();
            mc.selector(option);


        } while (!option.equals("x"));

    }


}