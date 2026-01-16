package br.ufrn.FormsFramework.model.interfaces;

import java.util.Map;

import org.springframework.stereotype.Component;

import br.ufrn.FormsFramework.model.Resposta;
import br.ufrn.FormsFramework.model.RespostaDissertativaCurta;
import br.ufrn.FormsFramework.model.RespostaDissertativaLonga;
import br.ufrn.FormsFramework.model.RespostaObjetivaMultipla;
import br.ufrn.FormsFramework.model.RespostaObjetivaSimples;
import br.ufrn.FormsFramework.model.enums.TipoResposta;

@Component
public abstract class Jorge {

    public void addRespostas(Map<String, Resposta> respostas) {
        respostas.put(TipoResposta.DISSERTATIVA_CURTA.toString(), new RespostaDissertativaCurta());
        respostas.put(TipoResposta.DISSERTATIVA_LONGA.toString(), new RespostaDissertativaLonga());
        respostas.put(TipoResposta.OBJETIVA_MULTIPLA.toString(), new RespostaObjetivaMultipla());
        respostas.put(TipoResposta.OBJETIVA_SIMPLES.toString(), new RespostaObjetivaSimples());
        
        addRespostasExtras(respostas);
    }


    public abstract void addRespostasExtras(Map<String, Resposta> respostas);
}