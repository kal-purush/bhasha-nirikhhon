package br.com.orion.bank.repository;

import java.math.BigDecimal;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import br.com.orion.bank.model.Card;

@Repository
public interface CardRepository extends JpaRepository<Card, Long> {

    @Query("from Card c where c.cardNumber = ?1 and c.securityCode = ?2")
    Optional<Card> findCard(Integer cardNumber, Integer securityCode);

    @Modifying
    @Query("update Card set creditAmount = (creditAmount - ?3) where cardNumber = ?1 and securityCode = ?2")
    void updateBalance(Integer cardNumber, Integer securityCode, BigDecimal valorCompra);

    @Query("select count(*) from Card c where c.cardNumber = ?1 and c.securityCode = ?2")
    Integer findValidCard(Integer cardNumber, Integer securityCode);

}