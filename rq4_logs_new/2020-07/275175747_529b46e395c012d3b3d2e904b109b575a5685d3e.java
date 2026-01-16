package jm.stockx.api.dao;

import jm.stockx.entity.Style;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public class StyleDaoImpl extends AbstractDAO<Style> implements StyleDAO {

    @Override
    public Optional<Style> getStyleByName(String name) {
        try {
            Style style = entityManager.createQuery("FROM Style WHERE name = :name", Style.class)
                    .setParameter("name", name)
                    .getSingleResult();
            return Optional.of(style);
        } catch (Exception e) {
            return Optional.empty();
        }
    }

}