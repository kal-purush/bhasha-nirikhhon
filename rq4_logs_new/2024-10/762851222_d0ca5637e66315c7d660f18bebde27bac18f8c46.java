package school.sptech.backend.domain.produtounitario.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import school.sptech.backend.service.produtounitario.view.QtdAtivoPorMes;

import java.time.LocalDate;
import java.util.List;


public interface QtdAtivoMesRepository extends JpaRepository<QtdAtivoPorMes, Integer> {

    List<QtdAtivoPorMes> findByProdutoIdAndCriadoEmBetween(Integer id, LocalDate inicio, LocalDate fim);
    @Query(
            "SELECT SUM(q.qtd)" +
                    " FROM QtdAtivoPorMes q"
    )
    Integer sumByCriadoEmBetween();
}