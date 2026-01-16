package br.com.alura.tdd.service;

import br.com.alura.tdd.modelo.Funcionario;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;

public class BonusServiceTeste {

    private BonusService service = new BonusService();

    @Test
    void  bonusDeveriaSerZeroParaFuncionarioComSalarioMuitoAlto(){
        BigDecimal bonus = service.calcularBonus(new Funcionario("jose", LocalDate.now(), BigDecimal.valueOf(25000.00)));

        Assertions.assertEquals(BigDecimal.ZERO,bonus);
    }

    @Test
    void  bonusDeveriaSerDezPorCentroDoSalarioDoFuncionario(){
        BigDecimal bonus = service.calcularBonus(new Funcionario("jose", LocalDate.now(), BigDecimal.valueOf(2500.00)));

        Assertions.assertEquals(BigDecimal.valueOf(250).setScale(2),bonus);
    }

    @Test
    void  bonusDeveriaSerMilParaFuncionarioComSalarioDeDezMil(){
        BigDecimal bonus = service.calcularBonus(new Funcionario("jose", LocalDate.now(), BigDecimal.valueOf(10000.00)));

        Assertions.assertEquals(BigDecimal.valueOf(1000).setScale(2),bonus);
    }


}