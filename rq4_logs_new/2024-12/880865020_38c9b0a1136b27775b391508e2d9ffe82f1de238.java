package com.web2.projetoweb2.repositorys;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.web2.projetoweb2.entity.Redirecionamento;

@Repository
public interface RedirecionamentoRepository extends JpaRepository<Redirecionamento, Integer> {
    List<Redirecionamento> findBySolicitacaoId(Integer solicitacaoId);
}