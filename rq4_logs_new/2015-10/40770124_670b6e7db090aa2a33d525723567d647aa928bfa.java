package co.uniandes.edu.ecos.services;

import co.uniandes.edu.ecos.negocio.ITramiteServiceLocal;
import co.uniandes.edu.service.Response.RespuestaTramite;
import javax.ejb.EJB;
import javax.jws.WebService;
import javax.jws.WebMethod;

/**
 *
 * @author Sergio Forero
 */
@WebService(serviceName = "TramiteServiceEmisor")
public class TramiteServiceEmisor {
    
    @EJB
    private ITramiteServiceLocal tramiteServiceBean;

    /**
     * Obtiene todos los tramites iniciados
     *
     * @param codigoEntidadEmisora
     * @return RespuestaTramite
     */
    @WebMethod(operationName = "obtenerTramitesPendientesEntidad")
    public RespuestaTramite obtenerTramitesPendientesEntidad(Integer codigoEntidadEmisora) {
        RespuestaTramite respuestaTramite = new RespuestaTramite();
        respuestaTramite = tramiteServiceBean.consultarTramiteEntidadEstado(codigoEntidadEmisora, 1);
        return respuestaTramite;
    }
      
}