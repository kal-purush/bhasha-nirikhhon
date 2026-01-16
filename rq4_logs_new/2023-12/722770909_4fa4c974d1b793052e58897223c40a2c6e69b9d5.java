package controller;

import model.PythonScrapers;
import view.MainForm;

import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MainFormController {
    private static MainForm mainForm;
    private static MainFormController mainFormController;
    private static PythonScrapers pythonScrapers = new PythonScrapers();


    public MainFormController() {
        control();
    }

    public static MainFormController getInstance() {
        if (Objects.isNull(mainFormController)) {
            mainForm = new MainForm();
            mainFormController = new MainFormController();
        }
        init();

        return mainFormController;
    }

    public void control() {
        mainForm.getButtonGenerate().addActionListener(e -> onButtonGenerate());
        mainForm.getTextFieldURL().getDocument().addDocumentListener(new DocumentListener() {
            public void changedUpdate(DocumentEvent e) {
                onChangeTextFields();
            }

            public void removeUpdate(DocumentEvent e) {
                onChangeTextFields();
            }

            public void insertUpdate(DocumentEvent e) {
                onChangeTextFields();
            }
        });

        }

    private void onChangeTextFields() {
        mainForm.getButtonGenerate().setEnabled(!mainForm.getTextFieldURL().getText().isEmpty());
        //mainForm.getButtonGenerate().setEnabled(!mainForm.getTextFieldURL().getText().contains("https://www.basketball-reference.com/boxscores/"));
    }

    private void onButtonGenerate() {
        try {
            pythonScrapers.runPythonScript("src/main/python/GenerateGameInserts.py", List.of(mainForm.getTextFieldURL().getText()));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void init() {
        mainForm.getFrame().setVisible(true);
    }
}