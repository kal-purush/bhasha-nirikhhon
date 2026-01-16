package br.ufrn.DASH.model.interfaces;

import org.mapstruct.ObjectFactory;

import br.ufrn.DASH.mapper.resposta.RespostaCreate;
import br.ufrn.DASH.mapper.resposta.RespostaUpdate;
import br.ufrn.DASH.model.Resposta;
import br.ufrn.DASH.model.RespostaDissertativaCurta;
import br.ufrn.DASH.model.enums.TipoResposta;

public class RespostaFactory {

    @ObjectFactory
    public static Resposta createResposta(RespostaCreate respostaCreate) {
        return createResposta(respostaCreate.tipoResposta());
    }

    @ObjectFactory
    public static Resposta createResposta(RespostaUpdate respostaUpdate) {
        return createResposta(respostaUpdate.tipoResposta());
    }

    private static Resposta createResposta(TipoResposta tipoResposta) {
        switch (tipoResposta) {
            case TipoResposta.DISSERTATIVA_CURTA:
                return new RespostaDissertativaCurta();
            default:
                throw new IllegalArgumentException("Tipo de resposta desconhecido: " + tipoResposta);
        }
    }
}