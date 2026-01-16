/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package controladores;

import daos.DaoCatalogo;
import modelos.Cuentas_Principales;

/**
 *
 * @author guill
 */
public class MIAU {

    public static void main(String[] args) {
        DaoCatalogo mi = new DaoCatalogo();
        Cuentas_Principales cuenta = mi.select_cod_principal("ACTIVO CORRIENTE");
        if (cuenta != null) {
            System.out.println("Código Principal: " + cuenta.getCod_Principal());
        } else {
            System.out.println("No se encontró ningún resultado.");
        }
    }

}