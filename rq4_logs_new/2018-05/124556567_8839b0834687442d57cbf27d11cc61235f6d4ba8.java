/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Model;

/**
 *
 * @author Yasmin
 */
public class VMTipoExamenExamen {
    int idExamen;
    String TipoExamne;
    String examenNombre;
    boolean visible;

    public VMTipoExamenExamen() {
    }

    public VMTipoExamenExamen(int idExamen, String TipoExamne, String examenNombre, boolean visible) {
        this.idExamen = idExamen;
        this.TipoExamne = TipoExamne;
        this.examenNombre = examenNombre;
        this.visible = visible;
    }

    public int getIdExamen() {
        return idExamen;
    }

    public String getTipoExamne() {
        return TipoExamne;
    }

    public String getExamenNombre() {
        return examenNombre;
    }

    public boolean isVisible() {
        return visible;
    }

    public void setIdExamen(int idExamen) {
        this.idExamen = idExamen;
    }

    public void setTipoExamne(String TipoExamne) {
        this.TipoExamne = TipoExamne;
    }

    public void setExamenNombre(String examenNombre) {
        this.examenNombre = examenNombre;
    }

    public void setVisible(boolean visible) {
        this.visible = visible;
    }
    
    
    
}