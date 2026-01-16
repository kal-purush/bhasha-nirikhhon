package dev.nar.projectrackr.dao;

import dev.nar.projectrackr.entities.ProjetEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProjetDao extends JpaRepository<ProjetEntity,Integer> {


}