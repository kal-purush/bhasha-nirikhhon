package Lesson_8;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class CalculaterListener implements ActionListener {
    private final JTextField textField;

    public CalculaterListener (JTextField textField) {
        this.textField = textField;
    }

    public void actionPerformed(ActionEvent actionEvent) {
        String value=textField.getText();
        String[] operators;
        if (value.contains("+")){
            operators=value.split("\\+");
            textField.setText(
                    String.valueOf(
                            Integer.parseInt(operators[0] )+ Integer.parseInt(operators[1])
                    )
            );
        }else if (value.contains("-")){
            operators=value.split("-");
            textField.setText(
                    String.valueOf(
                            Integer.parseInt(operators[0] )- Integer.parseInt(operators[1])
                    )
            );
        }else if (value.contains("*")){
            operators=value.split("\\*");
            textField.setText(
                    String.valueOf(
                            Integer.parseInt(operators[0]) * Integer.parseInt(operators[1])
                    )
            );
        }
    }
}