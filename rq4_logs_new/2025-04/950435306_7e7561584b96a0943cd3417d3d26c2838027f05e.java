package ru.nsu.peretyatko.repository.buildings;

import jakarta.persistence.EntityManager;
import jakarta.persistence.criteria.*;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import ru.nsu.peretyatko.model.buildings.Building;
import ru.nsu.peretyatko.model.infrastructure.Company;
import ru.nsu.peretyatko.model.infrastructure.Unit;

import java.util.List;

@Repository
@RequiredArgsConstructor
public class BuildingCustomRepository {

    private final EntityManager entityManager;

    public List<Building> findBuildingsUnit(int unitId) {
        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<Building> cq = cb.createQuery(Building.class);
        Root<Building> building = cq.from(Building.class);
        Join<Building, Unit> unit = building.join("unit");
        cq.select(building)
                .where(cb.equal(unit.get("id"), unitId));
        return entityManager.createQuery(cq).getResultList();
    }

    public List<Building> findBuildingsOfSeparation() {
        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<Building> cq = cb.createQuery(Building.class);
        Root<Building> building = cq.from(Building.class);
        Join<Building, Unit> unit = building.join("unit");
        Join<Unit, Company> company = unit.join("companies");
        cq.select(building)
                .groupBy(building.get("id"));
        return entityManager.createQuery(cq).getResultList();
    }

    public List<Building> findBuildingsOfNoSeparation() {
        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<Building> cq = cb.createQuery(Building.class);
        Root<Building> building = cq.from(Building.class);
        Subquery<Long> unitSubquery = cq.subquery(Long.class);
        Root<Unit> unit = unitSubquery.from(Unit.class);
        Join<Unit, Company> company = unit.join("companies");
        unitSubquery.select(unit.get("id"))
                .groupBy(unit.get("id"));
        cq.select(building)
                .where(cb.not(cb.in(building.get("unit").get("id")).value(unitSubquery)));
        return entityManager.createQuery(cq).getResultList();
    }
}