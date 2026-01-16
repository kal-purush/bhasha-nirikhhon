package UI;

import Generator.GeneratorRetranslator;
import Model.ControlTypes;

import java.util.*;

/**
 * This class offers to user working with application by the terminal.
 * @author Erokhin Yaroslav
 * @author Revtova Natalya
 * @author Timoshenskiy Georgy
 * @since 1.1
 */
public class CommandLineParser {

  private final Set<String> KINDOFCONTROLS = new HashSet<>(Arrays.asList("B", "Sp", "Sl", "TF", "CB", "RB"));
  private final int TOPLIMIT = 1000000;
  private final int OFFSETOFCONTROLS = 1;

  private int quantity;
  private boolean isDisabled, hasHighContrast, isUnsorted, hasNoise;
  private final Set<ControlTypes> controls = new HashSet<>(6);


  public CommandLineParser() {
  }

  /**
   * Method launches generator with terminal arguments.
   * @param args - arguments from terminal
   */
  public void parse(String[] args) {
    if (args.length == 1 && args[0].equals("help")) {
      help();
      return;
    }
    int numOfControls = Integer.valueOf(args[0]);
    if (numOfControls < 1 || numOfControls > 6) {
      throw new IllegalArgumentException("Incorrect number of controls");
    }

    if (args.length < 7 || args.length > 12 || (args.length - numOfControls) != 6) {
      throw new IllegalArgumentException("Incorrect number of arguments");
    }

    for (int i = OFFSETOFCONTROLS; i < OFFSETOFCONTROLS + numOfControls; ++i) {
      if (!KINDOFCONTROLS.contains(args[i])) {
        throw new IllegalArgumentException("Incorrect input of controls");
      } else {
        switch (args[i]) {
          case "B": controls.add(ControlTypes.BUTTON);
            break;
          case "Sp": controls.add(ControlTypes.SPINNER);
            break;
          case "Sl": controls.add(ControlTypes.SLIDER);
            break;
          case "TF": controls.add(ControlTypes.TEXTFIELD);
            break;
          case "CB": controls.add(ControlTypes.CHECKBOX);
            break;
          case "RB": controls.add(ControlTypes.RADIOBUTTON);
            break;
        }
      }
    }

    quantity = Integer.valueOf(args[OFFSETOFCONTROLS + numOfControls]);
    if (quantity > TOPLIMIT || quantity < 1) {
      throw new IllegalArgumentException("Incorrect quantity on controls");
    }

    ArrayList<String> allArgs = new ArrayList<String>(Arrays.asList(args));
    List<String> settings = allArgs.subList(OFFSETOFCONTROLS + numOfControls + 1, args.length);

    hasHighContrast = Boolean.valueOf(settings.get(0));
    isDisabled = Boolean.valueOf(settings.get(1));
    hasNoise = Boolean.valueOf(settings.get(2));
    isUnsorted = Boolean.valueOf(settings.get(3));

    ArrayList<ControlTypes> controlsList = new ArrayList<>(controls);

    GeneratorRetranslator generator = new GeneratorRetranslator();
    generator.startGenerator(controlsList, quantity, hasHighContrast, isDisabled, hasNoise, !isUnsorted, true, "dataset");
  }

  /**
   * Method offers help to user.
   */
  private void help() {
    String border = "=====================================================================================";
    String title = "AILearning API:\n";
    String template = "arguments: numOfControls, controls..., quantityOfEach, hasHighContrast, hasDisabled, " +
            "hasNoise, isUnsorted\n";
    String numOfCon= "numOfControls - integer number of controls (1 - 6)\n";
    String contrls = "controls... - each control is string and number of strings equals numOfControls";
    String namesOfControls = "Every string is define symbol:\nB - Button, TF - TextField, RB - RadioButton" +
            "Sp - Spinner, Sl - Slider, CB - CheckBox\n";
    String quantityOfEach = "quantityOfEach - integer number of each control (0 - 1 000 000)\n";
    String highContrast = "hasHighContrast - string (true or false)\n";
    String disabled = "hasDisabled - string (true of false)\nif true - some controls will be disabled\n";
    String noise = "hasNoise - string (true or false)\n";
    String unsorted = "isUnsorted - string (true or false)\nif true - controls will be in one folder\n";
    String example = "Example: 3 B TF RB 100 true true true false";
    String meaning = "It means that app will generate 100 Buttons, 100 TextFields, 100 RadioButtons " +
            "with high contrast, noise,\nsome controls will be disabled and locate in different folders";

    System.out.println(String.format("%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s", border, title, template, numOfCon,
            contrls, namesOfControls, quantityOfEach, highContrast, disabled, noise, unsorted, example, meaning));
  }

}