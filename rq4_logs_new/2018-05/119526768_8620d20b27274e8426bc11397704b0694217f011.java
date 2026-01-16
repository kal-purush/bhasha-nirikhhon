package es.uniovi.asw.e3b.agents_e3b.agents;

import org.springframework.http.ResponseEntity;

import es.uniovi.asw.e3b.agents_e3b.agents.web_service.request.PeticionInfoREST;
import es.uniovi.asw.e3b.agents_e3b.agents.web_service.responses.RespuestaInfoREST;

public interface GetAgentInfo {

	public ResponseEntity<RespuestaInfoREST> getPOSTpetition(PeticionInfoREST peticion);

}