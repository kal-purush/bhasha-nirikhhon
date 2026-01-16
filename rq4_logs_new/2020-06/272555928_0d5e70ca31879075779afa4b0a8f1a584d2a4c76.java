package br.com.orion.bank.service;

import java.math.BigDecimal;

import javax.transaction.Transactional;

import org.springframework.stereotype.Service;

import br.com.orion.bank.exceptions.PaymentException;
import br.com.orion.bank.model.Card;
import br.com.orion.bank.repository.CardRepository;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class CardService {

    private final CardRepository repository;

    public Card getCard(Integer cardNumber, Integer securityCode) {
        return repository.findCard(cardNumber, securityCode)
                .orElseThrow(() -> new PaymentException("Invalid card number."));
    }

    @Transactional
    public void atualizaSaldo(Integer cardNumber, Integer securityCode, BigDecimal billAmount) {
        repository.updateBalance(cardNumber, securityCode, billAmount);
    }

    public boolean isSaldoSuficiente(Integer cardNumber, Integer securityCode, BigDecimal billAmount) {
        Card card = getCard(cardNumber, securityCode);
        return card.getCreditAmount().compareTo(billAmount) >= 0;
    }

}