package com.bank;

import com.bank.controller.TransferButtonController;
import com.bank.model.dao.PersonDao;
import com.bank.model.service.PersonService;
import com.bank.model.service.validator.PersonValidator;
import com.bank.view.Window;

import javax.swing.*;

public class Main {

    public static void main(String[] args) {
        Window window = new Window("ATM", 800, 800);
        PersonDao personDao = new PersonDao();
        PersonValidator personValidator = new PersonValidator();
        PersonService personService = new PersonService(personDao, personValidator);
        TransferButtonController transferButtonController = new TransferButtonController(window, personService);
        initWindow(window, personService);
        window.init();

        window.getOperationButton().addActionListener(transferButtonController);
    }

    private static void initWindow(Window window, PersonService personService){
        JFrame frame = new JFrame();
        JPanel jPanel = new JPanel();
        JComboBox<String> accountFromSelector = new JComboBox<>();
        JComboBox<String> accountToSelector = new JComboBox<>();
        JTextField moneyField = new JTextField();
        JButton operationButton = new JButton();

        createAccountLabels(window, personService);

        window.setjFrame(frame);
        window.setPanel(jPanel);
        window.setAccountFromSelector(accountFromSelector);
        window.setAccountToSelector(accountToSelector);
        window.setMoneyField(moneyField);
        window.setOperationButton(operationButton);

        window.fillDropDown(personService.getAllIds(), accountFromSelector);
        window.fillDropDown(personService.getAllIds(), accountToSelector);

    }

    private static void createAccountLabels(Window window, PersonService personService){
        JLabel accountsLabel = new JLabel();
        window.setAccountsLabel(accountsLabel);
        window.addAccounts(personService.getAll());
    }
}