/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package minhavagaweb.controller;

import java.sql.SQLException;
import javax.servlet.http.HttpServletRequest;
import minhavagaweb.model.cdp.*;
import minhavagaweb.model.cgt.AplSolicitacao;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.ModelAndView;

/**
 *
 * @author ISM
 */
@Controller
public class ControllerSolicita {

    AplSolicitacao aplSolicitacao = new AplSolicitacao();
    Vaga vagaReservada;
    @RequestMapping(value = "encontrarVaga")
    public String encontrarVaga(@RequestParam("selectLocal") int estacionamento, @RequestParam("selectTipo") 
            int tipo) throws SQLException, ClassNotFoundException {
    	try {
    		vagaReservada = aplSolicitacao.solicita(estacionamento, tipo);
    		if (vagaReservada == null) {
    			throw new NullPointerException();
    		}
    	}
    	catch(NullPointerException n) {
    		return "vagaNaoEncontrada";
    	}
        return "confirmaSolicitacao";
    }
    
    @RequestMapping("solicitacaoConfirmada")
    public void confirmada() throws SQLException, ClassNotFoundException {
        Reserva reserva = new Reserva();
        reserva.setVagaReservada(vagaReservada);
    }
    

    @RequestMapping(value = "reservas")
    public void minhasReservas() throws SQLException, ClassNotFoundException {

        System.out.println("Minhas reservas");

    }

}