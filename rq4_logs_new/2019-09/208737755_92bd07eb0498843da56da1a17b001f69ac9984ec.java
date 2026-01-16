package main.repository;

import main.model.Morador;
import java.util.Date;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface MoradorRepositorio extends JpaRepository<Morador, Long>{
    @Modifying 
    @Query(value="Update morador SET nome=:nome, cpf=:cpf, rg=:rg, dt_nascimento=:dt_nascimento, email=:email, telefone=:telefone, id_unidade=:id_unidade where id=:id", nativeQuery = true)
    public void alterar(@Param("nome") String nome, @Param("cpf") String cpf, @Param("rg") String rg, 
            @Param("dt_nascimento") Date dt_nascimento, @Param("email") String email, @Param("telefone") String telefone, 
            @Param("id_unidade") Long id_unidade, @Param("id") Long id); 
}