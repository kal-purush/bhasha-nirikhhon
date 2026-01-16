package ru.nsu.peretyatko.repository.infrastructure;

import jakarta.persistence.EntityManager;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Join;
import jakarta.persistence.criteria.Root;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import ru.nsu.peretyatko.model.infrastructure.Division;
import ru.nsu.peretyatko.model.infrastructure.DivisionUnit;
import ru.nsu.peretyatko.model.infrastructure.Unit;

import java.util.List;

@Repository
@RequiredArgsConstructor
public class DivisionCustomRepository {

    private EntityManager entityManager;

}