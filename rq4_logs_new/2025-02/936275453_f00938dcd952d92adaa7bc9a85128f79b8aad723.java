package Than;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import javax.swing.text.AbstractDocument;
import javax.swing.text.DocumentFilter;
import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;

public class dacn extends JFrame {

    private JPanel contentPane;
    private JTextField textField;
    private JTextField textField_1;
    private JTextField textFieldResult; // Ô hiển thị kết quả

    public dacn() {
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setBounds(100, 100, 300, 380);
        contentPane = new JPanel();
        contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
        setContentPane(contentPane);
        contentPane.setLayout(null);

        // Ô nhập số thứ nhất
        textField = new JTextField();
        textField.setBounds(10, 10, 260, 40);
        contentPane.add(textField);
        textField.setColumns(10);

        // Ô nhập số thứ hai
        textField_1 = new JTextField();
        textField_1.setBounds(10, 60, 260, 40);
        contentPane.add(textField_1);
        textField_1.setColumns(10);

        // Thêm nhãn (label) cho ô kết quả
        JLabel lblResult = new JLabel("Kết quả:");
        lblResult.setBounds(10, 110, 100, 20);
        contentPane.add(lblResult);

        // Ô hiển thị kết quả
        textFieldResult = new JTextField();
        textFieldResult.setBounds(10, 130, 260, 40);
        textFieldResult.setEditable(false); // Không cho phép nhập dữ liệu
        contentPane.add(textFieldResult);
        textFieldResult.setColumns(10);

        // Chỉ cho phép nhập số vào hai ô đầu tiên
        ((AbstractDocument) textField.getDocument()).setDocumentFilter(new NumericFilter());
        ((AbstractDocument) textField_1.getDocument()).setDocumentFilter(new NumericFilter());

        // Nút Cộng (+)
        JButton btnAdd = new JButton("+");
        btnAdd.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                double num1 = Double.parseDouble(textField.getText());
                double num2 = Double.parseDouble(textField_1.getText());
                textFieldResult.setText(Double.toString(num1 + num2));
            }
        });
        btnAdd.setBounds(10, 180, 60, 40);
        contentPane.add(btnAdd);

        // Nút Trừ (-)
        JButton btnSubtract = new JButton("-");
        btnSubtract.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                double num1 = Double.parseDouble(textField.getText());
                double num2 = Double.parseDouble(textField_1.getText());
                textFieldResult.setText(Double.toString(num1 - num2));
            }
        });
        btnSubtract.setBounds(80, 180, 60, 40);
        contentPane.add(btnSubtract);

        // Nút Nhân (×)
        JButton btnMultiply = new JButton("×");
        btnMultiply.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                double num1 = Double.parseDouble(textField.getText());
                double num2 = Double.parseDouble(textField_1.getText());
                textFieldResult.setText(Double.toString(num1 * num2));
            }
        });
        btnMultiply.setBounds(150, 180, 60, 40);
        contentPane.add(btnMultiply);

        // Nút Chia (÷)
        JButton btnDivide = new JButton("÷");
        btnDivide.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                double num1 = Double.parseDouble(textField.getText());
                double num2 = Double.parseDouble(textField_1.getText());
                if (num2 != 0) {
                    textFieldResult.setText(Double.toString(num1 / num2));
                } else {
                    textFieldResult.setText("Lỗi chia 0");
                }
            }
        });
        btnDivide.setBounds(220, 180, 60, 40);
        contentPane.add(btnDivide);
    }

    // Bộ lọc chỉ cho phép nhập số
    class NumericFilter extends DocumentFilter {
        public void replace(javax.swing.text.DocumentFilter.FilterBypass fb, int offset, int length, String text, javax.swing.text.AttributeSet attrs)
                throws javax.swing.text.BadLocationException {
            if (text.matches("\\d*")) { // Chỉ cho phép nhập số
                super.replace(fb, offset, length, text, attrs);
            }
        }
    }
}