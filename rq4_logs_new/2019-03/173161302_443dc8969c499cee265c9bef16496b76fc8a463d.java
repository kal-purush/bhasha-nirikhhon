package lesson4;

import javax.swing.*;
import java.awt.*;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;


public class App extends JFrame {
    public App() throws HeadlessException {
        setTitle("My Chat");
        setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        setSize(400, 500);
        setResizable(true);

//        setLayout(new GridLayout(3, 1));

        JMenuBar menuBar = new JMenuBar();
        menuBar.setSize(400, 10);
        JMenu menu1 = new JMenu("Главная");
        JMenu menu2 = new JMenu("Контакты");
        menuBar.add(menu1);
        menuBar.add(menu2);

        JMenuItem menuItem11 = new JMenuItem("Открыть новое окно");
        JMenuItem menuItem12 = new JMenuItem("Сохранить");
        JMenuItem menuItem13 = new JMenuItem("Закрыть");
        menu1.add(menuItem11);
        menu1.add(menuItem12);
        menu1.add(menuItem13);

        JMenuItem menuItem21 = new JMenuItem("Найти новый контакт");
        JMenuItem menuItem22 = new JMenuItem("Удалить контакт");
        JMenuItem menuItem23 = new JMenuItem("Заблокировать контакт");
        menu2.add(menuItem21);
        menu2.add(menuItem22);
        menu2.add(menuItem23);

        getContentPane().add(BorderLayout.NORTH, menuBar);




        JPanel panel1 = new JPanel();
        panel1.setLayout(new GridLayout());
        JTextArea textArea = new JTextArea();
        panel1.add(new JScrollPane(textArea));
        panel1.setBackground(Color.cyan);// почему пропала заливка после добавления скролла?


        getContentPane().add(BorderLayout.CENTER, panel1);


        JPanel panel2 = new JPanel();
        panel2.setBackground(Color.lightGray);
        panel2.setLayout(new FlowLayout());
        JTextField textField = new JTextField(20);
        getContentPane().add(BorderLayout.SOUTH, panel2);

        JButton button = new JButton("Отправить");
        panel2.add(textField);
        panel2.add(button);


        textField.addActionListener(e -> {
            textField.getText();

            textField.setText("");

        });

        textField.addKeyListener(new KeyAdapter() {
            @Override
            public void keyReleased(KeyEvent e) {
                if (e.getKeyChar() == KeyEvent.VK_ENTER) {
                    textField.getText();
                    textField.setText("");
                }

            }
        });

        button.addActionListener(e -> {
            textArea.append(textField.getText()+ "\n\r");
        });


        setVisible(true);
    }



    public static void main(String[] args) {
        new App();
    }
}
