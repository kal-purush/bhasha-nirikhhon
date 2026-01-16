import java.io.*;
import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
public class AddOfTheStudents extends  Container{
    private JLabel nLabel;
    private JTextField name;
    private JLabel surLabel;
    private JTextField surName;
    private JLabel ageLabel;
    private JTextField age;
    private JButton back;
    private JButton add;

    public AddOfTheStudents(){

        setSize(500, 500);

        nLabel = new JLabel("NAME: ");
        nLabel.setSize(100, 30);
        nLabel.setLocation(100, 200);
        add(nLabel);

        name = new JTextField();
        name.setSize(200, 30);
        name.setLocation(200, 200);
        add(name);



        surLabel = new JLabel("SURNAME: ");
        surLabel.setSize(100, 30);
        surLabel.setLocation(100, 250);
        add(surLabel);

        surName = new JTextField();
        surName.setSize(200, 30);
        surName.setLocation(200, 250);
        add(surName);



        ageLabel = new JLabel("AGE: ");
        ageLabel.setSize(100, 30);
        ageLabel.setLocation(100, 300);
        add(ageLabel);

        age = new JTextField();
        age.setSize(200, 30);
        age.setLocation(200, 300);
        add(age);


        add = new JButton("ADD");
        add.setSize(90,30);
        add.setLocation(200,350);
        add(add);

        add.addActionListener(new ActionListener(){
            public void actionPerformed(ActionEvent e){
                String names = name.getText();
                String surNames = surName.getText();
                String ageString = age.getText();
                int ages = Integer.parseInt(ageString);
                refresh();
                Students students = new Students(names, surNames, ages);
                PackageData packageData = new PackageData("add", students);
                Client.sendPackage(packageData);
            }
        });
        back = new JButton("BACK");
        back.setSize(90,30);
        back.setLocation(310,350);
        add(back);

        back.addActionListener(new ActionListener(){
            public void actionPerformed(ActionEvent e){
                Client.frame.showMenuPage();
            }
        });
    }

    public void refresh(){
        name.setText(" ");
        surName.setText(" ");
        age.setText(" ");
    }



}