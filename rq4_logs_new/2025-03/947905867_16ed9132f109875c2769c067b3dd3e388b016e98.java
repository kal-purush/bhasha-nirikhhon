package com.pet.pet_funeral.domain.pet_funeral.repository;

import com.pet.pet_funeral.domain.pet_funeral.model.PetFuneral;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface PetFuneralRepository extends JpaRepository<PetFuneral, UUID> {
    Page<PetFuneral> findByAddress_City(String addressCity, Pageable pageable);
}