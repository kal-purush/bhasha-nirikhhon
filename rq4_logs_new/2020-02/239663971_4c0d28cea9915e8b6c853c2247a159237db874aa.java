package pe.edu.ceid.simi.operative.application.apertura;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import pe.edu.ceid.simi.operative.domain.apertura.model.Apertura;
import pe.edu.ceid.simi.operative.domain.apertura.model.AperturaDTO;
import pe.edu.ceid.simi.operative.domain.apertura.repository.AperturaRepository;

@Service
public class AperturaServiceImpl implements AperturaService{

	private AperturaRepository repository;
	
	@Override
	public List<AperturaDTO> getAperturas() {
		return this.repository.getAperturas();
	}

	@Override
	public boolean saveAperturas(List<Apertura> aperturas) {
		return this.saveAperturas(aperturas);
	}
	

}