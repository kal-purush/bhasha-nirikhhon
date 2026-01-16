/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Controlador.seguridad;

/**
 *
 * @author Soporte
 */
public class UsuarioConectado {
    private static int idUsuario;
    private static String userName;

    public static int getIdUsuario() {
        return idUsuario;
    }

    public static void setIdUsuario(int idUsuario) {
        UsuarioConectado.idUsuario = idUsuario;
    }

    public static String getUserName() {
        return userName;
    }

    public static void setUserName(String userName) {
        UsuarioConectado.userName = userName;
    }
    
    
}