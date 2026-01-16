package pe.edu.ceid.simi.operative.application.autenticacion;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import pe.edu.ceid.simi.operative.domain.autorizacion.model.Autorizacion;
import pe.edu.ceid.simi.operative.domain.autorizacion.repository.AutorizacionRepository;

@Service
public class AutenticacionServiceImpl implements AutenticacionService{

	@Autowired
	private AutorizacionRepository repository;
	
	public Autorizacion validarParametrosAutorizacion(String codigo, String tipo) {
		return this.repository.validarParametrosAutorizacion(codigo, tipo);
	}

	@Override
	public boolean accederSistema(String codigo, String tipo) {
		Autorizacion autorizacion = this.validarParametrosAutorizacion(codigo, tipo);
		if (autorizacion != null) {
			if (!autorizacion.getCodigo().equals("0")) {
				return true;
			}
		}
		return false;
	}

}