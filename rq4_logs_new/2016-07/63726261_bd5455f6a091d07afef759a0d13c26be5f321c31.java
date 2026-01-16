/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ejemplo_uso_webservice_java.Ejemplos;

import ejemplo_uso_webservice_java.lib.ClienteCobroDigital;
import java.util.HashMap;
import java.util.LinkedHashMap;

/**
 *
 * @author ariel
 */
public class consultar_transacciones {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        String sid = "MeAOO0d8tpk87Ud3AG0mZO7WCIP76GuKfU48UMVCuLO66aQGa0Iw3R6cDVs";
        String mercalpha = "CI366779";

        String desde = "20160607";
        String hasta = "20160715";
        LinkedHashMap filtros = new LinkedHashMap();
        try {
            ClienteCobroDigital cobro_digital = new ClienteCobroDigital(mercalpha, sid);
            if (!cobro_digital.consultar_transacciones(desde, hasta, filtros)) {
                System.out.println(cobro_digital.obtener_log());
            } else {
                System.out.println(cobro_digital.obtener_datos());
            }
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            System.out.println(ex.getCause());
            System.out.println(ex.getLocalizedMessage());
            System.out.println(ex.getStackTrace());
            System.out.println(ex.getSuppressed());
        }

    }
}