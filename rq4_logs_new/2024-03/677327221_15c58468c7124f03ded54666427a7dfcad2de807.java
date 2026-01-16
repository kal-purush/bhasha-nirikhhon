package com.br.agilize.dash.repository.dashboardRepository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.time.LocalDateTime;
import java.util.Optional;

import com.br.agilize.dash.model.entity.ColaboradorEntity;
import com.br.agilize.dash.model.entity.dashboardEntity.PrCountEntity;

@Repository
public interface PrCountRepository extends JpaRepository<PrCountEntity, Long> {
    
    PrCountEntity findByColaborador(ColaboradorEntity colaborador);
}