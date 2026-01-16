package ru.clevertec.carshowroom.repository.car.impl;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.JoinType;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;
import org.hibernate.Hibernate;
import org.hibernate.Session;
import org.hibernate.query.Query;
import ru.clevertec.carshowroom.entity.CarEntity;
import ru.clevertec.carshowroom.entity.CarShowroomEntity;
import ru.clevertec.carshowroom.entity.CategoryEntity;
import ru.clevertec.carshowroom.exception.CategoryNotFoundException;
import ru.clevertec.carshowroom.repository.car.CarRepository;
import ru.clevertec.carshowroom.util.HibernateUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class CarRepositoryImpl implements CarRepository {

    @Override
    public CarEntity addCar(CarEntity carEntity) {
        try (Session session = HibernateUtil.getSession()) {
            session.beginTransaction();
            session.persist(carEntity);
            session.getTransaction().commit();
        }
        return carEntity;
    }

    @Override
    public List<CarEntity> findAll() {
        try (Session session = HibernateUtil.getSession()) {
            session.beginTransaction();
            Query<CarEntity> query = session.createQuery(
                    "SELECT c FROM CarEntity c " +
                            "JOIN FETCH c.categoryEntity " +
                            "LEFT JOIN FETCH c.carShowroomEntity " +
                            "LEFT JOIN FETCH c.clientEntities",
                    CarEntity.class
            );

            List<CarEntity> carEntities = query.getResultList();

            carEntities.forEach(carEntity -> Hibernate.initialize(carEntity.getReviewEntities()));

            session.getTransaction().commit();
            return carEntities;
        }
    }

    @Override
    public Optional<CarEntity> findCarById(Long id) {
        try (Session session = HibernateUtil.getSession()) {
            session.beginTransaction();
            Query<CarEntity> query = session.createQuery(
                    "SELECT c FROM CarEntity c " +
                            "JOIN FETCH c.categoryEntity " +
                            "LEFT JOIN FETCH c.carShowroomEntity " +
                            "LEFT JOIN FETCH c.clientEntities " +
                            "WHERE c.id = :id",
                    CarEntity.class
            );
            query.setParameter("id", id);
            Optional<CarEntity> optionalCarEntity = query.list().stream().findAny();
            optionalCarEntity.ifPresent(carEntity -> Hibernate.initialize(carEntity.getReviewEntities()));
            session.getTransaction().commit();
            return optionalCarEntity;
        }
    }

    @Override
    public Optional<CarEntity> update(CarEntity updatedCarEntity) {
        try (Session session = HibernateUtil.getSession()) {
            session.beginTransaction();
            CarEntity existingCarEntity = session.find(CarEntity.class, updatedCarEntity.getId());
            if (existingCarEntity == null) {
                session.getTransaction().rollback();
                return Optional.empty();
            }

            existingCarEntity.setModel(updatedCarEntity.getModel());
            existingCarEntity.setBrand(updatedCarEntity.getBrand());
            existingCarEntity.setYear(updatedCarEntity.getYear());
            existingCarEntity.setPrice(updatedCarEntity.getPrice());

            if (!existingCarEntity.getCategoryEntity().equals(updatedCarEntity.getCategoryEntity())) {
                existingCarEntity.setCategoryEntity(updatedCarEntity.getCategoryEntity());
            }

            session.merge(existingCarEntity);
            session.getTransaction().commit();

            return Optional.of(existingCarEntity);
        }
    }

    @Override
    public boolean deleteById(Long id) {
        try (Session session = HibernateUtil.getSession()) {
            session.beginTransaction();
            CarEntity carEntity = session.find(CarEntity.class, id);
            if (carEntity != null) {
                session.remove(carEntity);
                session.getTransaction().commit();
                return true;
            }
            session.getTransaction().commit();
            return false;
        }
    }

    @Override
    public void assignCarToShowroom(CarEntity carEntity, CarShowroomEntity carShowroomEntity) {
        try (Session session = HibernateUtil.getSession()) {
            session.beginTransaction();

            if (carEntity.getCategoryEntity().getId() != null) {
                CategoryEntity categoryEntity = session.find(CategoryEntity.class, carEntity.getCategoryEntity().getId());
                if (categoryEntity == null) {
                    throw CategoryNotFoundException.byDefault();
                }
                carEntity.setCategoryEntity(categoryEntity);
            }
            carEntity.setCarShowroomEntity(carShowroomEntity);
            session.merge(carEntity);
            session.getTransaction().commit();
        }
    }

    @Override
    public List<CarEntity> findCarsByFilters(String brand, String category, Integer minPrice, Integer maxPrice) {
        try (Session session = HibernateUtil.getSession()) {
            session.beginTransaction();

            CriteriaBuilder cb = session.getCriteriaBuilder();
            CriteriaQuery<CarEntity> cq = cb.createQuery(CarEntity.class);
            Root<CarEntity> car = cq.from(CarEntity.class);

            car.fetch("clientEntities", JoinType.LEFT);
            car.fetch("reviewEntities", JoinType.LEFT);

            List<Predicate> predicates = new ArrayList<>();

            if (brand != null) {
                predicates.add(cb.equal(car.get("brand"), brand));
            }

            if (category != null) {
                predicates.add(cb.equal(car.join("categoryEntity").get("name"), category));
            }
            if (minPrice != null) {
                predicates.add(cb.ge(car.get("price"), minPrice));
            }
            if (maxPrice != null) {
                predicates.add(cb.le(car.get("price"), maxPrice));
            }

            cq.where(cb.and(predicates.toArray(new Predicate[0])));
            Query<CarEntity> query = session.createQuery(cq);

            List<CarEntity> results = query.getResultList();
            session.getTransaction().commit();

            return results;
        }
    }

    @Override
    public List<CarEntity> findCarsSortedByPrice(boolean ascending) {
        try (Session session = HibernateUtil.getSession()) {
            session.beginTransaction();

            CriteriaBuilder cb = session.getCriteriaBuilder();
            CriteriaQuery<CarEntity> cq = cb.createQuery(CarEntity.class);
            Root<CarEntity> car = cq.from(CarEntity.class);

            car.fetch("clientEntities", JoinType.LEFT);
            car.fetch("reviewEntities", JoinType.LEFT);

            if (ascending) {
                cq.orderBy(cb.asc(car.get("price")));
            } else {
                cq.orderBy(cb.desc(car.get("price")));
            }

            Query<CarEntity> query = session.createQuery(cq);

            List<CarEntity> results = query.getResultList();
            session.getTransaction().commit();

            return results;
        }
    }

    @Override
    public List<CarEntity> findCarsWithPagination(int pageNumber, int pageSize) {
        try (Session session = HibernateUtil.getSession()) {
            session.beginTransaction();

            CriteriaBuilder cb = session.getCriteriaBuilder();
            CriteriaQuery<CarEntity> cq = cb.createQuery(CarEntity.class);
            Root<CarEntity> car = cq.from(CarEntity.class);

            car.fetch("clientEntities", JoinType.LEFT);
            car.fetch("reviewEntities", JoinType.LEFT);

            cq.select(car);

            Query<CarEntity> query = session.createQuery(cq);
            query.setFirstResult((pageNumber - 1) * pageSize);
            query.setMaxResults(pageSize);

            List<CarEntity> results = query.getResultList();
            session.getTransaction().commit();

            return results;
        }
    }
}