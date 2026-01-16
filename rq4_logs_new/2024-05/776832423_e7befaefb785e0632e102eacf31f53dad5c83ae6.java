package com.fivetraining.app.commands;

import com.fivetraining.app.daos.SubscriptionDAO;
import com.fivetraining.app.models.Subscription;
import com.fivetraining.console.ConsoleInteraction;
import com.fivetraining.console.exceptions.ConsoleCommandExecutionException;
import com.fivetraining.console.items.ConsoleCommand;

import java.sql.SQLException;
import java.util.List;

public class ListSubscriptionsCommand extends ConsoleCommand {
    private final SubscriptionDAO subscriptionDAO;

    public ListSubscriptionsCommand(SubscriptionDAO subscriptionDAO) {
        this.subscriptionDAO = subscriptionDAO;
    }

    @Override
    public String getName() {
        return "list-inscs";
    }

    @Override
    public void run(ConsoleInteraction interaction) throws ConsoleCommandExecutionException {
        try {
            List<Subscription> subscriptions = subscriptionDAO.findAll();

            for (Subscription subscription : subscriptions) {
                interaction.getConsole().writeLine();
                interaction.getConsole().writeLine("o  id: " + subscription.getId());
                interaction.getConsole().writeLine("|  id do aluno: " + subscription.getUserId());
                interaction.getConsole().writeLine("|  código do plano: " + subscription.getPlanCode());
                interaction.getConsole().writeLine("|  data de início: " + subscription.getStartDate());
                interaction.getConsole().writeLine("|  data de fim: " + subscription.getEndDate());
                interaction.getConsole().writeLine("|  número do cartão: " + subscription.getCreditCard().getNumber());
                interaction.getConsole().writeLine("|  data de expiração do cartão: " + subscription.getCreditCard().getExpiryDate());
                interaction.getConsole().writeLine("`- cvc do cartão: " + subscription.getCreditCard().getCvv());
            }
        } catch (SQLException exception) {
            throw new ConsoleCommandExecutionException(exception.getMessage());
        }
    }
}