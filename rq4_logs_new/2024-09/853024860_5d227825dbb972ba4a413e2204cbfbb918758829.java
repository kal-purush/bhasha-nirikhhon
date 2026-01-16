package br.inatel;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class BuscaHorarioDeAtendimento {
        horarioDeAtendimentoService horarioDeAtendimentoService;

        public BuscaHorarioDeAtendimento(horarioDeAtendimentoService horarioDeAtendimentoService) {
            this.horarioDeAtendimentoService = horarioDeAtendimentoService;
        }

        public horarioDeAtendimento buscaHorarioDeAtendimento(String id) {
           String horarioJson = String.valueOf(horarioDeAtendimentoService.findById(Integer.parseInt(id)));

        }




    }