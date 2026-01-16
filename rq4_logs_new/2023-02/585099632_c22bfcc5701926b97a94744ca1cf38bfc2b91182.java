package task_10;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

/**
 * Доработать фрейм из урока. Добавить кнопки для вычисления
 * операции сложения, вычитания, умножения и деления.
 * Результат нужно вывести на сам фрейм в компоненте
 * JLabel(используйте метод setText)
 */

public class MyFrame extends JFrame {
    JLabel text1, text2;
    JTextField field1, field2;
    JButton buttonPlus, buttonMinus, buttonMultiply, buttonDivide;

    MyFrame() {
        setTitle("Calculator");
        setSize(400, 300);
        setLocationRelativeTo(null); //вывод окна по центру

        text1 = getLable("First number");
        text2 = getLable("Second number");

        field1 = new JTextField(10);
        field2 = new JTextField(10);

        buttonPlus = new JButton("+");
        buttonMinus = new JButton("-");
        buttonMultiply = new JButton("*");
        buttonDivide = new JButton("/");

        Listener listener = new Listener();

        buttonPlus.addActionListener(listener);
        buttonMinus.addActionListener(listener);
        buttonMultiply.addActionListener(listener);
        buttonDivide.addActionListener(listener);

        Container container = getContentPane();
        container.setLayout(new FlowLayout(FlowLayout.CENTER, 20, 20));

        container.add(text1);
        container.add(field1);
        container.add(text2);
        container.add(field2);
        container.add(buttonPlus);
        container.add(buttonMinus);
        container.add(buttonMultiply);
        container.add(buttonDivide);


        setVisible(true);
    }

    class Listener implements ActionListener {

        @Override
        public void actionPerformed(ActionEvent actionEvent) {
            if (actionEvent.getSource() == buttonPlus) {
                int result = Integer.parseInt(field1.getText()) + Integer.parseInt(field2.getText());
                JOptionPane.showMessageDialog(null, result);
            } else if (actionEvent.getSource() == buttonMinus) {
                int result = Integer.parseInt(field1.getText()) - Integer.parseInt(field2.getText());
                JOptionPane.showMessageDialog(null, result);
            } else if (actionEvent.getSource() == buttonMultiply) {
                int result = Integer.parseInt(field1.getText()) * Integer.parseInt(field2.getText());
                JOptionPane.showMessageDialog(null, result);
            } else if (actionEvent.getSource() == buttonDivide) {
                int result = Integer.parseInt(field1.getText()) / Integer.parseInt(field2.getText());
                JOptionPane.showMessageDialog(null, result);
            }
        }
    }


    static JLabel getLable(String text) {
        var lable = new JLabel(text);
        lable.setFont(new Font("arial", Font.BOLD | Font.ITALIC, 30));
        lable.setForeground(Color.ORANGE);
        lable.setToolTipText("Подсказка ...");
        return lable;
    }

    public static void main(String[] args) {
        var obj = new MyFrame();
    }
}