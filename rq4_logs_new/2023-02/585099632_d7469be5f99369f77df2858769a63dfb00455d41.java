package task_11;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

/**
 * 1. Изменить внешний вид, приблизить внешний вид к Windows калькулятору.
 * 2. Добавьте пару новых операций, например корень квадратный и число в степени 2 и степени 3.
 * 3. Должна быть возможность выполнять действия с числами, в которых больше одной цифры.
 * 4. Добавить меню с возможностью отображать описание калькулятора.
 */

public class Calculator extends JFrame {
    private JTextArea text;                    //поле для ввода цифр
    private double number1 = 0, number2 = 0;   //переменные для хранения операндов
    private int oper;                          //номер операции

    Calculator() {
        var menuBar = new JMenuBar();
        JMenu mainMenu = new JMenu("Главное меню");
        var fontBar = new Font("serif", Font.BOLD, 15);
        mainMenu.setFont(fontBar);

        var menuInner1 = new JMenu("Операции");
        var menuInner2 = new JMenu("Числа");
        var menuInner3 = new JMenu("Тип калькулятора");
        mainMenu.add(menuInner1);
        mainMenu.add(menuInner2);
        mainMenu.add(menuInner3);
        var item1 = new JMenuItem("Сложение");
        var item2 = new JMenuItem("Вычитание");
        var item3 = new JMenuItem("Умножение");
        var item4 = new JMenuItem("Деление");
        item1.setFont(fontBar);
        item2.setFont(fontBar);
        item3.setFont(fontBar);
        item4.setFont(fontBar);
        menuInner1.add(item1);
        menuInner1.add(item2);
        menuInner1.add(item3);
        menuInner1.add(item4);
        var item5 = new JMenuItem("Числа с плавающей точкой");
        var item6 = new JMenuItem("Целые числа");
        item5.setFont(fontBar);
        item6.setFont(fontBar);
        menuInner2.add(item5);
        menuInner2.add(item6);
        var item7 = new JMenuItem("Простой");
        var item8 = new JMenuItem("Инженерный");
        item7.setFont(fontBar);
        item8.setFont(fontBar);
        menuInner3.add(item7);
        menuInner3.add(item8);
        mainMenu.addSeparator();
        var item45 = new JMenuItem("Выход");
        item45.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                System.exit(0);
            }
        });
        mainMenu.add(item45);
        menuBar.add(mainMenu);
        setJMenuBar(menuBar);

        var c = getContentPane();
        var panel = new JPanel();
        panel.setLayout(null);                                         //настройка вручную местоположения элементов

        JButton buttons[] = new JButton[20];
        var font = new Font("serif", Font.ITALIC, 15);

        for (int i = 0; i < buttons.length; i++) {
            buttons[i] = new JButton();
            buttons[i].setFont(font);                                 //задаем шрифт текста в кнопке
            if (i < 5) {
                buttons[i].setLocation(20 + 120 * i, 100);      //задаем отступы у кнопок
            } else if (i < 10) {
                buttons[i].setLocation(20 + 120 * (i - 5), 150);//задаем отступы у кнопок
            } else if (i < 15) {
                buttons[i].setLocation(20 + 120 * (i - 10), 200);
            } else {
                buttons[i].setLocation(20 + 120 * (i - 15), 250);
            }
            buttons[i].setSize(100, 25);               //размер кнопки
            buttons[i].setBackground(Color.DARK_GRAY);             //цвет фона
            buttons[i].setForeground(Color.WHITE);                 //цвет текста
            buttons[i].addActionListener(new ActionListener() {
                @Override
                public void actionPerformed(ActionEvent e) {
                    JButton button = (JButton) e.getSource();      //получили кнопку, на которую был клик
                    String textButton = button.getText();
                    if (textButton.equalsIgnoreCase("Выход")) {
                        System.exit(0);
                    } else if (textButton.equalsIgnoreCase("C")) {
                        text.setText("");                          //очищаем текстовое поле
                        number1 = 0;
                        number2 = 0;
                    } else if (textButton.equalsIgnoreCase("=")) {
                        number2 = Double.parseDouble(text.getText());
                        if (oper == 1) {
                            text.setText("" + (number1 + number2));
                        } else if (oper == 2) {
                            text.setText("" + (number1 - number2));
                        } else if (oper == 3) {
                            text.setText("" + (number1 * number2));
                        } else if (oper == 4) {
                            if (number2 == 0) {
                                JOptionPane.showMessageDialog(null, "На 0 делить нельзя!");
                            } else {
                                text.setText("" + (number1 / number2));
                            }
                        }

                    } else if (textButton.equalsIgnoreCase("+")) {
                        oper = 1;
                        number1 = Double.parseDouble(text.getText());
                        text.setText("+");
                    } else if (textButton.equalsIgnoreCase("-")) {
                        oper = 2;
                        number1 = Double.parseDouble(text.getText());
                        text.setText("-");
                    } else if (textButton.equalsIgnoreCase("*")) {
                        oper = 3;
                        number1 = Double.parseDouble(text.getText());
                        text.setText("*");
                    } else if (textButton.equalsIgnoreCase("/")) {
                        oper = 4;
                        number1 = Double.parseDouble(text.getText());
                        text.setText("/");
                    } else if (textButton.equalsIgnoreCase("^2")) {
                        oper = 5;
                        number1 = Double.parseDouble(text.getText());
                        text.setText("" + (number1 * number1));
                    } else if (textButton.equalsIgnoreCase("^3")) {
                        oper = 6;
                        number1 = Double.parseDouble(text.getText());
                        text.setText("" + (number1 * number1 * number1));
                    } else if (textButton.equalsIgnoreCase("√")) {
                        oper = 7;
                        number1 = Double.parseDouble(text.getText());
                        text.setText("" + (Math.sqrt(number1)));
                    } else {
                        text.setText(textButton);//клик на цифру

                    }

                }
            });
            panel.add(buttons[i]);//размещаем каждую кнопку в панельке
        }

        for (int i = 0; i < 10; i++) {
            buttons[i].setText("" + i);//
        }
        buttons[10].setText("+");
        buttons[11].setText("-");
        buttons[12].setText("*");
        buttons[13].setText("/");
        buttons[14].setText("=");
        buttons[15].setText("^2");
        buttons[16].setText("^3");
        buttons[17].setText("√");
        buttons[18].setText("C");
        buttons[19].setText("Выход");

        var res = new JLabel("Результат");
        res.setFont(new Font("serif", Font.ITALIC | Font.BOLD, 30));
        res.setBounds(130, 0, 300, 50);
        panel.add(res);

        text = new JTextArea();
        text.setFont(new Font("serif", Font.BOLD, 30));
        text.setBounds(300, 10, 300, 35);
        panel.add(text);

        c.add(panel);

        setSize(700, 400);
        setTitle("Калькулятор v1.0");
        setLocationRelativeTo(null);
        setResizable(false);//запрещаем менять размер окна
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);//закрытие по крестику
        setVisible(true);
    }

    public static void main(String[] args) {
        new Calculator().setVisible(true);
    }


}