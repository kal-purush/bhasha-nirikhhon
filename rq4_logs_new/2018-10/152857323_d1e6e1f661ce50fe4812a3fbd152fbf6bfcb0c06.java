package com.bank.controller;

import com.bank.model.exception.AccountIsTheSameException;
import com.bank.model.exception.IncorrectBalanceException;
import com.bank.model.service.LogService;
import com.bank.model.service.PersonService;
import com.bank.view.Window;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;


public class WithdrawalButtonController implements ActionListener {

    private Window window;
    private PersonService personService;
    private LogService logService = LogService.getInstance();

    public WithdrawalButtonController(Window window, PersonService personService) {
        this.window = window;
        this.personService = personService;
    }

    @Override
    public void actionPerformed(ActionEvent e) {
        try {
            int account = Integer.parseInt(String.valueOf(window.getAccountSelector().getSelectedItem()));
            int sum = Integer.parseInt(String.valueOf(window.getMoneyField().getText()));
            personService.withdrawal(account, sum);
            window.refresh(personService.getAll());
        } catch (NumberFormatException | IncorrectBalanceException ex) {
            JOptionPane.showMessageDialog(window.getjFrame(), ex.getMessage());
            logService.error("Some problem happened", ex);
        }
    }

}