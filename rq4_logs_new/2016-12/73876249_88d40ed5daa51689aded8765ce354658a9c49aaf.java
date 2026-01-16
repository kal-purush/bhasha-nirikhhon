package dto;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

public class VehiculoTerceroDTO implements Serializable {

	private static final long serialVersionUID = 1L;

	private int idVehiculoTercero;
	private String tipoVehiculo;
	private float precio;
	private String estado;
	private Date fechaLlegada;
	private List<PedidoDTO> pedidos;

	public VehiculoTerceroDTO() {
		super();
	}

	public VehiculoTerceroDTO(int idVehiculoTercero, String tipoVehiculo,
			float precio, String estado, Date fechaLlegada, List<PedidoDTO> pedidos) {
		super();
		this.idVehiculoTercero = idVehiculoTercero;
		this.tipoVehiculo = tipoVehiculo;
		this.precio = precio;
		this.estado = estado;
		this.pedidos = pedidos;
		this.fechaLlegada = fechaLlegada;
	}

	public int getIdVehiculoTercero() {
		return idVehiculoTercero;
	}

	public void setIdVehiculoTercero(int idVehiculoTercero) {
		this.idVehiculoTercero = idVehiculoTercero;
	}

	public String getTipoVehiculo() {
		return tipoVehiculo;
	}

	public void setTipoVehiculo(String tipoVehiculo) {
		this.tipoVehiculo = tipoVehiculo;
	}

	public float getPrecio() {
		return precio;
	}

	public void setPrecio(float precio) {
		this.precio = precio;
	}

	public String getEstado() {
		return estado;
	}

	public void setEstado(String estado) {
		this.estado = estado;
	}

	public List<PedidoDTO> getPedidos() {
		return pedidos;
	}

	public void setPedidos(List<PedidoDTO> pedidos) {
		this.pedidos = pedidos;
	}

	public Date getFechaLlegada() {
		return fechaLlegada;
	}

	public void setFechaLlegada(Date fechaLlegada) {
		this.fechaLlegada = fechaLlegada;
	}
}