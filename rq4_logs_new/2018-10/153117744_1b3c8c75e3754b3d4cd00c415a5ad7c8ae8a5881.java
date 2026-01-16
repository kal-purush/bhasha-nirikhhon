package com.evtimov.landlordapp.landlordspring.services;

import com.evtimov.landlordapp.landlordspring.models.Card;
import com.evtimov.landlordapp.landlordspring.repositories.base.CardRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class CardService {
    private final CardRepository repository;

    @Autowired
    public CardService(CardRepository repository) {
        this.repository = repository;
    }

    public void addCard(Card card) {

    }

    public void deleteCard(int id){

    }

    public List<Card> getAllCardsByUser(int id) {
        return null;
    }


}