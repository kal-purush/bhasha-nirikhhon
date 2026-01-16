package es.uji.ei1027.toopots.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

import es.uji.ei1027.toopots.dao.ActividadDao;
import es.uji.ei1027.toopots.dao.ClienteDao;
import es.uji.ei1027.toopots.dao.MonitorDao;
import es.uji.ei1027.toopots.dao.TipoActividadDao;

@Controller
@RequestMapping("/administrador")
public class AdministradorController {
	
	private MonitorDao monitorDao;
	private ActividadDao actividadDao;
	private TipoActividadDao tipoActividadDao;
	private ClienteDao clienteDao;

    @Autowired
    public void setMonitorDao(MonitorDao monitorDao) {
        this.monitorDao=monitorDao;
    }
    
	@Autowired
	public void setActividadDao(ActividadDao actividadDao) {
		this.actividadDao = actividadDao; 
	}
	
	@Autowired
	public void setTipoActividadDao(TipoActividadDao tipoActividadDao) {
		this.tipoActividadDao = tipoActividadDao; 
	}

	@Autowired
	public void setClienteDao(ClienteDao clienteDao) {
		this.clienteDao=clienteDao;
	}
	
	
	@RequestMapping("/lobby")
	public String administrador(Model model) {
		return "admin/lobby";
	}
	
	
	
	
	

}