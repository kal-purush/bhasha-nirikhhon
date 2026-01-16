package com.mikaelsonbraz.parkingapi.api.ordered.model.repository;

import com.mikaelsonbraz.parkingapi.api.ordered.model.entity.Ordered;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderedRepository extends JpaRepository<Ordered, Integer> {
}