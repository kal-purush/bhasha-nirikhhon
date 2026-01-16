package org.amine.accountservice.commands.aggregates;

import org.amine.accountservice.commonapi.commands.CreateAccountCommand;
import org.amine.accountservice.commonapi.enums.AccountStatus;
import org.amine.accountservice.commonapi.events.AccountCreatedEvent;
import org.amine.accountservice.commonapi.exceptions.NegativeInitialBalanceException;
import org.axonframework.commandhandling.CommandHandler;
import org.axonframework.eventsourcing.EventSourcingHandler;
import org.axonframework.modelling.command.AggregateIdentifier;
import org.axonframework.modelling.command.AggregateLifecycle;
import org.axonframework.spring.stereotype.Aggregate;

@Aggregate
public class AccountAggregate {
    @AggregateIdentifier
    private String accountId;
    private String currency;
    private double balance;
    private AccountStatus status;

    public AccountAggregate() {
        // required by Axon
    }

    @CommandHandler
    public AccountAggregate(CreateAccountCommand command) {
        // TODO: règles métier ici (fonction de décision)
        if (command.getInitialBalance() < 0) throw new NegativeInitialBalanceException("Negative balance");

        // On crée l'événement comme quoi le compte a été crée -> il est enregistré dans le event store
        AggregateLifecycle.apply(new AccountCreatedEvent(
                command.getId(),
                command.getCurrency(),
                command.getInitialBalance(),
                AccountStatus.CREATED));


        // TODO: Il faut informer les autres ms (fonction d'évolution)
    }

    @EventSourcingHandler
    public void on(AccountCreatedEvent event) {
        // ça veut dire je vais dans le eventStore, à chaque fois
        // qu'il y a un event de type 'AccountCreatedEvent' je fais ce qui suit
        this.accountId = event.getId();
        this.balance = event.getBalance();
        this.status = event.getStatus();
        this.currency = event.getCurrency();
    }
}