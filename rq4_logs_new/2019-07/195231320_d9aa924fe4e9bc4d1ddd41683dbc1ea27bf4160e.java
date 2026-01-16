package Model;

import javax.swing.*;
import java.awt.*;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

public enum ControlTypes {
    CHECKBOX,
    TEXTFIELD,
    RADIOBUTTON,
    SPINNER,
    SLIDER,
    BUTTON;

    private static final ControlTypes[] ENUM_VALUES = ControlTypes.values();
    private static final int SIZE = ENUM_VALUES.length;
    private final int MIN_CONTROL_VALUE_SIZE = 3;
    private final int MAX_CONTROL_VALUE_SIZE = 12;

    private ArrayList<Integer> sliderValues = new ArrayList<>();

    private final String LETTERS = "abcdefghijklmnopqrstuvxyz";
    private final char[] CHARS = (LETTERS + LETTERS.toUpperCase() + "0123456789").toCharArray();

    public Component getObject() {
        switch (this) {
            case CHECKBOX: {
                return new JCheckBox(getRandomString(), getRandomBool());
            }
            case TEXTFIELD: {
                return new JTextField(getRandomString());
            }
            case RADIOBUTTON: {
                return new JRadioButton(getRandomString(), getRandomBool());
            }
            case SPINNER: {
                JSpinner spinner = new JSpinner();
                spinner.setValue(getRandomInt(Integer.MIN_VALUE, Integer.MAX_VALUE - 1));
                return spinner;
            }
            case SLIDER: {
                sliderValues.clear();
                for (int i = 5; i < 11; i += 5) {
                    sliderValues.add(getRandomInt(i - 5, i) * 10 + i * 2 - 10);
                }
                sliderValues.add(getRandomInt(sliderValues.get(0), sliderValues.get(1)));
                JSlider slider = new JSlider(JSlider.HORIZONTAL,
                        sliderValues.get(0), sliderValues.get(1), sliderValues.get(2));
                if (sliderValues.get(1) - sliderValues.get(0) > 50) {
                    slider.setMajorTickSpacing(20);
                    slider.setMinorTickSpacing(10);
                } else {
                    slider.setMajorTickSpacing(10);
                    slider.setMinorTickSpacing(5);
                }
                slider.setPaintTicks(true);
                slider.setPaintLabels(true);
                return slider;
            }
            case BUTTON: {
                return new JButton(getRandomString());
            }
            default: {
                return null;
            }
        }
    }

    public int getRandomInt(int min, int max) {
        return ThreadLocalRandom.current().nextInt(min, max + 1);
    }
    public boolean getRandomBool(){
        return ThreadLocalRandom.current().nextBoolean();
    }
    public String getRandomString() {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < getRandomInt(MIN_CONTROL_VALUE_SIZE, MAX_CONTROL_VALUE_SIZE); i++) {
            result.append(CHARS[ThreadLocalRandom.current().nextInt(CHARS.length)]);
        }
        return result.toString();
    }
    public ControlTypes getRandomObject() {
        return ENUM_VALUES[getRandomInt(0, SIZE - 1)];
    }
    public String getFolderName() {
        return this.name().toLowerCase();
    }
}