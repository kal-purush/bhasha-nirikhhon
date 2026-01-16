package br.ufrn.instancies.DASH.service;

import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import br.ufrn.FormsFramework.model.Formulario;
import br.ufrn.FormsFramework.service.EstatisticasLLM;

@Component
public class EstatisticasDeSintomasDash implements EstatisticasLLM {
    
    public Map<String, String> gerarRespostaLLM(List<Formulario> formularios) {
        return Map.of("content", "DASH");
    }

    public String gerarPrompt(List<Formulario> formularios) {
        return "DASH";
    }

}