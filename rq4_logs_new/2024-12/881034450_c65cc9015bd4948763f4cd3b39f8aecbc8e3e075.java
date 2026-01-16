package org.example.restaurantvoting.restaurant.service;

import jakarta.persistence.EntityManagerFactory;
import org.example.restaurantvoting.common.exception.AppException;
import org.example.restaurantvoting.common.exception.ErrorType;
import org.example.restaurantvoting.restaurant.model.Restaurant;
import org.hibernate.envers.AuditReader;
import org.hibernate.envers.AuditReaderFactory;
import org.hibernate.envers.query.AuditEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class RestaurantAuditService {

    @Autowired
    private EntityManagerFactory entityManagerFactory;

    @Transactional
    public Restaurant getPreviousRestaurantVersion(int restaurantId) {
        final AuditReader auditReader = AuditReaderFactory.get(entityManagerFactory.createEntityManager());

        List<Restaurant> restaurants = auditReader.createQuery()
                .forRevisionsOfEntity(Restaurant.class, true, true)
                .add(AuditEntity.id().eq(restaurantId))
                .getResultList();

        if (restaurants.size() > 1) {
            return restaurants.get(restaurants.size() - 2);
        } else {
            throw new AppException("No previous versions were found!", ErrorType.NOT_FOUND);
        }
    }
}