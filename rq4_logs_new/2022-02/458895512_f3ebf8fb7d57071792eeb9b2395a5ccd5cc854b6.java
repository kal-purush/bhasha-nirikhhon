package ru.zhbert.docslinter.domain;

import java.util.ArrayList;

public class DictTerm {

    String mainForm, firstFromLineForm;
    ArrayList<String> wrongForms;

    public DictTerm(String mainForm, String firstFromLineForm, ArrayList<String> wrongForms) {
        this.mainForm = mainForm;
        this.firstFromLineForm = firstFromLineForm;
        this.wrongForms = wrongForms;
    }

    public DictTerm() {
    }

    public String getMainForm() {
        return mainForm;
    }

    public void setMainForm(String mainForm) {
        this.mainForm = mainForm;
    }

    public String getFirstFromLineForm() {
        return firstFromLineForm;
    }

    public void setFirstFromLineForm(String firstFromLineForm) {
        this.firstFromLineForm = firstFromLineForm;
    }

    public ArrayList<String> getWrongForms() {
        return wrongForms;
    }

    public void setWrongForms(ArrayList<String> wrongForms) {
        this.wrongForms = wrongForms;
    }
}