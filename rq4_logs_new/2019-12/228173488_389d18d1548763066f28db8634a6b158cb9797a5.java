package View;

import Model.PIMCollection;
import Model.PIMContact;
import Model.PIMEntity;

import javax.swing.*;
import java.awt.*;

public class addContact extends JDialog {
    String User;
    PIMCollection<PIMEntity> pimCollection;

    JTextArea first = new JTextArea(1, 20);
    JTextArea last = new JTextArea(1, 20);
    JTextArea email = new JTextArea(1, 20);
    JTextArea priority = new JTextArea(1, 20);
    JRadioButton pri;
    JRadioButton pub;

    public addContact(JFrame parent, String user, PIMCollection<PIMEntity> PIM) {
        super(parent, true);
        this.setLocation(0, parent.getHeight() - 390);
        User = user;
        pimCollection = PIM;

        init();
    }

    private void init() {
        //属性设置
        this.setTitle("新建联系人");
        this.setLayout(new BorderLayout());
        this.setSize(330, 360);
        this.setResizable(false);
        this.setDefaultCloseOperation(WindowConstants.HIDE_ON_CLOSE);

        JPanel content = new JPanel(new GridBagLayout());
        GridBagConstraints constraints = new GridBagConstraints();
        constraints.insets = new Insets(10, 10, 10, 10);

        constraints.gridx = 0;
        constraints.gridy = 0;
        constraints.gridwidth = 1;
        content.add(new JLabel("姓：", JLabel.RIGHT), constraints);

        constraints.gridx = 1;
        constraints.gridy = 0;
        constraints.gridwidth = 3;
        content.add(first, constraints);

        constraints.gridx = 0;
        constraints.gridy = 1;
        constraints.gridwidth = 1;
        content.add(new JLabel("名：", JLabel.RIGHT), constraints);

        constraints.gridx = 1;
        constraints.gridy = 1;
        constraints.gridwidth = 3;
        content.add(last, constraints);

        constraints.gridx = 0;
        constraints.gridy = 2;
        constraints.gridwidth = 1;
        content.add(new JLabel("E-mail：", JLabel.RIGHT), constraints);

        constraints.gridx = 1;
        constraints.gridy = 2;
        constraints.gridwidth = 3;
        content.add(email, constraints);

        constraints.gridx = 0;
        constraints.gridy = 3;
        constraints.gridwidth = 1;
        content.add(new JLabel("优先级：", JLabel.RIGHT), constraints);

        constraints.gridx = 1;
        constraints.gridy = 3;
        constraints.gridwidth = 3;
        content.add(priority, constraints);

        constraints.gridx = 0;
        constraints.gridy = 4;
        constraints.gridwidth = 1;
        content.add(new JLabel("权限：", JLabel.RIGHT), constraints);

        JPanel setprivatre = new JPanel(new FlowLayout(FlowLayout.LEFT, 30, 5));
        if (User.isBlank()) {
            pri = new JRadioButton("私有");
            pub = new JRadioButton("公开", true);
        } else {
            pri = new JRadioButton("私有", true);
            pub = new JRadioButton("公开");
        }
        ButtonGroup group = new ButtonGroup();
        group.add(pri);
        group.add(pub);
        setprivatre.add(pri);
        setprivatre.add(pub);

        constraints.gridx = 1;
        constraints.gridy = 4;
        constraints.gridwidth = 3;
        content.add(setprivatre, constraints);

        this.add(content, BorderLayout.CENTER);

        //窗口底部的“提交”和“取消”按钮，同时为它们设置动作监听器
        JPanel foot = new JPanel(new FlowLayout(FlowLayout.CENTER, 40, 5));
        JButton submit = new JButton("提交");
        submit.addActionListener(e -> {
            PIMContact contact = new PIMContact();
            String status = "contact";

            if (first.getText().isBlank() && last.getText().isBlank())
                status = "empty";

            switch (status) {
                case "contact":
                    contact.setOwner(User);
                    String s = first.getText() + " " + last.getText() + " " + email.getText();
                    contact.fromString(s);
                    contact.setPriority(priority.getText().isBlank() ? "common" : priority.getText());
                    contact.setPrivate(pri.isSelected());
                    pimCollection.add(contact);
                    pimCollection.save();
                    cleanContent();
                    break;
                case "empty":
                    JOptionPane.showMessageDialog(this, "请输入联系人姓名！", "添加失败", JOptionPane.ERROR_MESSAGE);
                    break;
                default:
                    JOptionPane.showMessageDialog(this, "未知错误……", "添加失败", JOptionPane.ERROR_MESSAGE);
            }
        });
        JButton cancel = new JButton("取消");
        cancel.addActionListener(e -> cleanContent());
        ButtonGroup footButton = new ButtonGroup();
        footButton.add(submit);
        footButton.add(cancel);
        foot.add(submit);
        foot.add(cancel);
        this.add(foot, BorderLayout.SOUTH);
    }

    private void cleanContent() {
        first.setText("");
        last.setText("");
        email.setText("");
        priority.setText("");
        pri.setSelected(!User.isBlank());
        pub.setSelected(User.isBlank());

        this.setVisible(false);
    }
}