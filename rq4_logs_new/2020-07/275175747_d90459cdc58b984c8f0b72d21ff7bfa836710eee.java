package jm.stockx.api.dao;

import jm.stockx.entity.Currency;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public class CurrencyDAOImpl extends AbstractDAO<Currency, Long> implements CurrencyDAO {

    @Override
    public Optional<Currency> getByName(String name) {
        Currency currency = entityManager.createQuery("FROM Currency WHERE name = :currencyName", Currency.class)
                .setParameter("currencyName", name)
                .getSingleResult();
        return Optional.of(currency);
    }
}