/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package compras.controlador;



/**
 *
 * @author visitante
 */
public class clsCompras {
    //declaracion de variable para Compras 
    int coedocumento;
    int procodigo;
    String coefecha;
    int coetotal;
    String coeestatus;
    int codorden;
    int prodcodigo;
    int codcantidad;
    int codcosto;
    int bodcodigo;


    public clsCompras() {
    }

    public clsCompras(int coedocumento) {
    this.coedocumento = coedocumento;
    }

    public clsCompras(int coedocumento, int procodigo, String coefecha, int coetotal, String coeestatus, int codorden, int prodcodigo, int codcantidad, int codcosto, int bodcodigo) {
        this.coedocumento = coedocumento;
        this.procodigo = procodigo;
        this.coefecha = coefecha;
        this.coetotal = coetotal;
        this.coeestatus = coeestatus;
        this.codorden = codorden;
        this.prodcodigo = prodcodigo;
        this.codcantidad = codcantidad;
        this.codcosto = codcosto;
        this.bodcodigo = bodcodigo;
    }

    public int getCoedocumento() {
        return coedocumento;
    }

    public void setCoedocumento(int coedocumento) {
        this.coedocumento = coedocumento;
    }

    public int getProcodigo() {
        return procodigo;
    }

    public void setProcodigo(int procodigo) {
        this.procodigo = procodigo;
    }

    public String getCoefecha() {
        return coefecha;
    }

    public void setCoefecha(String coefecha) {
        this.coefecha = coefecha;
    }

    public int getCoetotal() {
        return coetotal;
    }

    public void setCoetotal(int coetotal) {
        this.coetotal = coetotal;
    }

    public String getCoeestatus() {
        return coeestatus;
    }

    public void setCoeestatus(String coeestatus) {
        this.coeestatus = coeestatus;
    }

    public int getCodorden() {
        return codorden;
    }

    public void setCodorden(int codorden) {
        this.codorden = codorden;
    }

    public int getProdcodigo() {
        return prodcodigo;
    }

    public void setProdcodigo(int prodcodigo) {
        this.prodcodigo = prodcodigo;
    }

    public int getCodcantidad() {
        return codcantidad;
    }

    public void setCodcantidad(int codcantidad) {
        this.codcantidad = codcantidad;
    }

    public int getCodcosto() {
        return codcosto;
    }

    public void setCodcosto(int codcosto) {
        this.codcosto = codcosto;
    }

    public int getBodcodigo() {
        return bodcodigo;
    }

    public void setBodcodigo(int bodcodigo) {
        this.bodcodigo = bodcodigo;
    }

    

    @Override
    public String toString() {
         return "tbl_compras_encabezado {" + "coedocumento =" + coedocumento + ", procodigo=" + procodigo + ", coefecha=" + coefecha + ", coetotal=" + coetotal +", coeestatus=" + coeestatus + ", prodcodigo=" + prodcodigo +", codcantidad=" + codcantidad +", codcosto=" + codcosto +", bodcodigo=" + bodcodigo + '}';
         
    }
    
}