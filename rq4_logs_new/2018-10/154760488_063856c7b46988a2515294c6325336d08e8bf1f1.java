package com.company;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

class Editor extends JFrame implements ActionListener {
    JMenuBar menu;
    JMenu file;
    JMenuItem _open;
    JMenuItem _new;
    JMenuItem _save;
    JTextArea area;
    File files;


    public Editor() {
        files = new File("");


        menu = new JMenuBar();

        file = menu.add(new JMenu("File"));

        _new = file.add(new JMenuItem("New file"));
        _new.addActionListener(this);

        _open = file.add(new JMenuItem("Open"));
        _open.addActionListener(this);

        _save = file.add(new JMenuItem("Save"));
        _save.addActionListener(this);

        setSize(800, 600);
        setTitle("Text_Editor");
        setDefaultCloseOperation(EXIT_ON_CLOSE);
        setJMenuBar(menu);
        setVisible(true);


        area = new JTextArea();
        area.setSize(700, 500);
        add(area);


    }


    @Override
    public void actionPerformed(ActionEvent e) {
        switch (e.getActionCommand()) {
            case "New file":
                JFileChooser openfile2 = new JFileChooser();
                if (openfile2.showSaveDialog(null) == JFileChooser.APPROVE_OPTION) {
                    files = openfile2.getSelectedFile();
                    try {
                        FileWriter writer = new FileWriter(files);
                        writer.write("");
                        writer.flush();
                    } catch (Exception e2) {

                    }
                }
                break;

            case "Open":
                JFileChooser openfile = new JFileChooser();
                openfile.showOpenDialog(this);
                files = openfile.getSelectedFile();

                try {
                    FileReader reader = new FileReader(files);
                    char[] buffer = new char[(int) files.length()];
                    reader.read(buffer);
                    area.setText(new String(buffer));
                } catch (Exception e2) {

                }
                break;
            case "Save":

                try {
                    FileWriter writer = new FileWriter(files);
                    writer.write(area.getText());
                    writer.flush();
                    area.setText("");
                } catch (Exception e2) {

                }
                break;

        }
    }
}


