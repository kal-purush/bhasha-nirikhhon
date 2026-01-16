package pe.edu.upc.project_security_g06.dtos;

public class CantidadRelacionContactosDTO {
    private  String relacion_contacto;
    private int cantidad_relaciones_similares;



    public String getRelacion_contacto() {
        return relacion_contacto;
    }
    public void setRelacion_contacto(String relacion_contacto) {
        this.relacion_contacto = relacion_contacto;
    }
    public int getCantidad_relaciones_similares() {
        return cantidad_relaciones_similares;
    }
    public void setCantidad_relaciones_similares(int cantidad_relaciones_similares) {
        this.cantidad_relaciones_similares = cantidad_relaciones_similares;
    }
}