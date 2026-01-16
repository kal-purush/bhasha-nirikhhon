package ru.nsu.peretyatko.repository.equipments;

import jakarta.persistence.EntityManager;
import jakarta.persistence.criteria.*;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import ru.nsu.peretyatko.model.equipments.Equipment;
import ru.nsu.peretyatko.model.equipments.EquipmentCategory;
import ru.nsu.peretyatko.model.equipments.EquipmentType;
import ru.nsu.peretyatko.model.infrastructure.*;

import java.util.List;

@Repository
@RequiredArgsConstructor
public class EquipmentCustomRepository {

    private final EntityManager entityManager;

    public List<Equipment> findEquipmentsByType(String titleType) {
        CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Equipment> criteriaQuery = criteriaBuilder.createQuery(Equipment.class);
        Root<Equipment> militaryEquipmentRoot = criteriaQuery.from(Equipment.class);
        Join<Equipment, EquipmentType> equipmentTypeJoin = militaryEquipmentRoot.join("type");
        criteriaQuery.select(militaryEquipmentRoot)
                .where(criteriaBuilder.equal(equipmentTypeJoin.get("title"), titleType));
        return entityManager.createQuery(criteriaQuery).getResultList();
    }

    public List<Equipment> findEquipmentsByCategory(String titleCategory) {
        CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Equipment> criteriaQuery = criteriaBuilder.createQuery(Equipment.class);
        Root<Equipment> equipmentRoot = criteriaQuery.from(Equipment.class);
        Join<Equipment, EquipmentType> equipmentTypeJoin = equipmentRoot.join("type");
        Join<EquipmentType, EquipmentCategory> equipmentCategoryJoin = equipmentTypeJoin.join("category");
        criteriaQuery.select(equipmentRoot)
                .where(criteriaBuilder.equal(equipmentCategoryJoin.get("title"), titleCategory));
        return entityManager.createQuery(criteriaQuery).getResultList();
    }

    public List<Equipment> findEquipmentsByTypeUnit(String titleType, int unitId) {
        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<Equipment> cq = cb.createQuery(Equipment.class);
        Root<Equipment> equipment = cq.from(Equipment.class);
        Join<Equipment, EquipmentType> typeJoin = equipment.join("type");
        Join<Equipment, Unit> unitJoin = equipment.join("unit");
        cq.select(equipment)
                .where(cb.and(
                        cb.equal(unitJoin.get("id"), unitId),
                        cb.equal(typeJoin.get("title"), titleType)
                ));
        return entityManager.createQuery(cq).getResultList();
    }

    public List<Equipment> findEquipmentsByCategoryUnit(String titleCategory, int unitId) {
        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<Equipment> cq = cb.createQuery(Equipment.class);
        Root<Equipment> equipment = cq.from(Equipment.class);
        Join<Equipment, EquipmentType> typeJoin = equipment.join("type");
        Join<EquipmentType, EquipmentCategory> categoryJoin = typeJoin.join("category");
        Join<Equipment, Unit> unitJoin = equipment.join("unit");
        cq.select(equipment)
                .where(cb.and(
                        cb.equal(unitJoin.get("id"), unitId),
                        cb.equal(categoryJoin.get("title"), titleCategory)
                ));
        return entityManager.createQuery(cq).getResultList();
    }

    public List<Equipment> findEquipmentsByTypeDivision(String titleType, int divisionId) {
        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<Equipment> cq = cb.createQuery(Equipment.class);
        Root<Equipment> equipment = cq.from(Equipment.class);
        Join<Equipment, EquipmentType> type = equipment.join("type");
        Join<Equipment, Unit> unit = equipment.join("unit");
        Join<Unit, DivisionUnit> divisionUnit = unit.join("divisionUnits");
        Join<DivisionUnit, Division> division = divisionUnit.join("division");
        cq.select(equipment)
                .where(cb.and(
                        cb.equal(type.get("title"), titleType),
                        cb.equal(division.get("id"), divisionId)
                ));
        return entityManager.createQuery(cq).getResultList();
    }

    public List<Equipment> findEquipmentsByCategoryDivision(String titleCategory, int divisionId) {
        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<Equipment> cq = cb.createQuery(Equipment.class);
        Root<Equipment> equipment = cq.from(Equipment.class);
        Join<Equipment, EquipmentType> type = equipment.join("type");
        Join<EquipmentType, EquipmentCategory> category = type.join("category");
        Join<Equipment, Unit> unit = equipment.join("unit");
        Join<Unit, DivisionUnit> divisionUnit = unit.join("divisionUnits");
        Join<DivisionUnit, Division> division = divisionUnit.join("division");
        cq.select(equipment)
                .where(cb.and(
                        cb.equal(category.get("title"), titleCategory),
                        cb.equal(division.get("id"), divisionId)
                ));
        return entityManager.createQuery(cq).getResultList();
    }

    public List<Equipment> findEquipmentsByTypeBrigade(String titleType, int brigadeId) {
        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<Equipment> cq = cb.createQuery(Equipment.class);
        Root<Equipment> equipment = cq.from(Equipment.class);
        Join<Equipment, EquipmentType> type = equipment.join("type");
        Join<Equipment, Unit> unit = equipment.join("unit");
        Join<Unit, BrigadeUnit> brigadeUnit = unit.join("brigadeUnits");
        Join<BrigadeUnit, Brigade> brigade = brigadeUnit.join("brigade");
        cq.select(equipment)
                .where(cb.and(
                        cb.equal(type.get("title"), titleType),
                        cb.equal(brigade.get("id"), brigadeId)
                ));
        return entityManager.createQuery(cq).getResultList();
    }

    public List<Equipment> findEquipmentsByCategoryBrigade(String titleCategory, int brigadeId) {
        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<Equipment> cq = cb.createQuery(Equipment.class);
        Root<Equipment> equipment = cq.from(Equipment.class);
        Join<Equipment, EquipmentType> type = equipment.join("type");
        Join<EquipmentType, EquipmentCategory> category = type.join("category");
        Join<Equipment, Unit> unit = equipment.join("unit");
        Join<Unit, BrigadeUnit> brigadeUnit = unit.join("brigadeUnits");
        Join<BrigadeUnit, Brigade> brigade = brigadeUnit.join("brigade");
        cq.select(equipment)
                .where(cb.and(
                        cb.equal(category.get("title"), titleCategory),
                        cb.equal(brigade.get("id"), brigadeId)
                ));
        return entityManager.createQuery(cq).getResultList();
    }

    public List<Equipment> findEquipmentsByTypeCorps(String titleType, int corpsId) {
        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<Equipment> cq = cb.createQuery(Equipment.class);
        Root<Equipment> equipment = cq.from(Equipment.class);
        Join<Equipment, EquipmentType> type = equipment.join("type");
        Join<Equipment, Unit> unit = equipment.join("unit");
        Join<Unit, CorpsUnit> corpsUnit = unit.join("corpsUnits");
        Join<CorpsUnit, Corps> corps = corpsUnit.join("corps");
        cq.select(equipment)
                .where(cb.and(
                        cb.equal(type.get("title"), titleType),
                        cb.equal(corps.get("id"), corpsId)
                ));
        return entityManager.createQuery(cq).getResultList();
    }

    public List<Equipment> findEquipmentsByCategoryCorps(String titleCategory, int corpsId) {
        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<Equipment> cq = cb.createQuery(Equipment.class);
        Root<Equipment> equipment = cq.from(Equipment.class);
        Join<Equipment, EquipmentType> type = equipment.join("type");
        Join<EquipmentType, EquipmentCategory> category = type.join("category");
        Join<Equipment, Unit> unit = equipment.join("unit");
        Join<Unit, CorpsUnit> corpsUnit = unit.join("corpsUnits");
        Join<CorpsUnit, Corps> corps = corpsUnit.join("corps");
        cq.select(equipment)
                .where(cb.and(
                        cb.equal(category.get("title"), titleCategory),
                        cb.equal(corps.get("id"), corpsId)
                ));
        return entityManager.createQuery(cq).getResultList();
    }

    public List<Equipment> findEquipmentsByTypeArmy(String titleType, int armyId) {
        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<Equipment> cq = cb.createQuery(Equipment.class);
        Subquery<Equipment> divisionSubquery = cq.subquery(Equipment.class);
        Root<Equipment> divisionEquipment = divisionSubquery.from(Equipment.class);
        Join<Equipment, EquipmentType> divisionType = divisionEquipment.join("type");
        Join<Equipment, Unit> divisionUnit = divisionEquipment.join("unit");
        Join<Unit, DivisionUnit> divisionUnitJoin = divisionUnit.join("divisionUnits");
        Join<DivisionUnit, Division> division = divisionUnitJoin.join("division");
        Join<Division, ArmyDivision> armyDivision = division.join("armyDivisions");
        divisionSubquery.select(divisionEquipment)
                .where(cb.and(
                        cb.equal(divisionType.get("title"), titleType),
                        cb.equal(armyDivision.get("army").get("id"), armyId)
                ));
        Subquery<Equipment> brigadeSubquery = cq.subquery(Equipment.class);
        Root<Equipment> brigadeEquipment = brigadeSubquery.from(Equipment.class);
        Join<Equipment, EquipmentType> brigadeType = brigadeEquipment.join("type");
        Join<Equipment, Unit> brigadeUnit = brigadeEquipment.join("unit");
        Join<Unit, BrigadeUnit> brigadeUnitJoin = brigadeUnit.join("brigadeUnits");
        Join<BrigadeUnit, Brigade> brigade = brigadeUnitJoin.join("brigade");
        Join<Brigade, ArmyBrigade> armyBrigade = brigade.join("armyBrigades");
        brigadeSubquery.select(brigadeEquipment)
                .where(cb.and(
                        cb.equal(brigadeType.get("title"), titleType),
                        cb.equal(armyBrigade.get("army").get("id"), armyId)
                ));
        Subquery<Equipment> corpsSubquery = cq.subquery(Equipment.class);
        Root<Equipment> corpsEquipment = corpsSubquery.from(Equipment.class);
        Join<Equipment, EquipmentType> corpsType = corpsEquipment.join("type");
        Join<Equipment, Unit> corpsUnit = corpsEquipment.join("unit");
        Join<Unit, CorpsUnit> corpsUnitJoin = corpsUnit.join("corpsUnits");
        Join<CorpsUnit, Corps> corps = corpsUnitJoin.join("corps");
        Join<Corps, ArmyCorps> armyCorps = corps.join("armyCorps");
        corpsSubquery.select(corpsEquipment)
                .where(cb.and(
                        cb.equal(corpsType.get("title"), titleType),
                        cb.equal(armyCorps.get("army").get("id"), armyId)
                ));
        cq.select(cq.from(Equipment.class))
                .where(cb.or(
                        cb.in(cq.from(Equipment.class)).value(divisionSubquery),
                        cb.in(cq.from(Equipment.class)).value(brigadeSubquery),
                        cb.in(cq.from(Equipment.class)).value(corpsSubquery)
                ));
        return entityManager.createQuery(cq).getResultList();
    }

    public List<Equipment> findEquipmentsByCategoryArmy(String titleCategory, int armyId) {
        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<Equipment> cq = cb.createQuery(Equipment.class);
        Subquery<Equipment> divisionSubquery = cq.subquery(Equipment.class);
        Root<Equipment> divisionEquipment = divisionSubquery.from(Equipment.class);
        Join<Equipment, EquipmentType> divisionType = divisionEquipment.join("type");
        Join<EquipmentType, EquipmentCategory> divisionCategory = divisionType.join("category");
        Join<Equipment, Unit> divisionUnit = divisionEquipment.join("unit");
        Join<Unit, DivisionUnit> divisionUnitJoin = divisionUnit.join("divisionUnits");
        Join<DivisionUnit, Division> division = divisionUnitJoin.join("division");
        Join<Division, ArmyDivision> armyDivision = division.join("armyDivisions");
        divisionSubquery.select(divisionEquipment)
                .where(cb.and(
                        cb.equal(divisionCategory.get("title"), titleCategory),
                        cb.equal(armyDivision.get("army").get("id"), armyId)
                ));
        Subquery<Equipment> brigadeSubquery = cq.subquery(Equipment.class);
        Root<Equipment> brigadeEquipment = brigadeSubquery.from(Equipment.class);
        Join<Equipment, EquipmentType> brigadeType = brigadeEquipment.join("type");
        Join<EquipmentType, EquipmentCategory> brigadeCategory = brigadeType.join("category");
        Join<Equipment, Unit> brigadeUnit = brigadeEquipment.join("unit");
        Join<Unit, BrigadeUnit> brigadeUnitJoin = brigadeUnit.join("brigadeUnits");
        Join<BrigadeUnit, Brigade> brigade = brigadeUnitJoin.join("brigade");
        Join<Brigade, ArmyBrigade> armyBrigade = brigade.join("armyBrigades");
        brigadeSubquery.select(brigadeEquipment)
                .where(cb.and(
                        cb.equal(brigadeCategory.get("title"), titleCategory),
                        cb.equal(armyBrigade.get("army").get("id"), armyId)
                ));
        Subquery<Equipment> corpsSubquery = cq.subquery(Equipment.class);
        Root<Equipment> corpsEquipment = corpsSubquery.from(Equipment.class);
        Join<Equipment, EquipmentType> corpsType = corpsEquipment.join("type");
        Join<EquipmentType, EquipmentCategory> corpsCategory = corpsType.join("category");
        Join<Equipment, Unit> corpsUnit = corpsEquipment.join("unit");
        Join<Unit, CorpsUnit> corpsUnitJoin = corpsUnit.join("corpsUnits");
        Join<CorpsUnit, Corps> corps = corpsUnitJoin.join("corps");
        Join<Corps, ArmyCorps> armyCorps = corps.join("armyCorps");
        corpsSubquery.select(corpsEquipment)
                .where(cb.and(
                        cb.equal(corpsCategory.get("title"), titleCategory),
                        cb.equal(armyCorps.get("army").get("id"), armyId)
                ));
        cq.select(cq.from(Equipment.class))
                .where(cb.or(
                        cb.in(cq.from(Equipment.class)).value(divisionSubquery),
                        cb.in(cq.from(Equipment.class)).value(brigadeSubquery),
                        cb.in(cq.from(Equipment.class)).value(corpsSubquery)
                ));
        return entityManager.createQuery(cq).getResultList();
    }
}