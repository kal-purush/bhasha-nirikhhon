package Controlador;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Utilidades {

    // Método para encriptar la contraseña con SHA-256 //IMPLEMENTADO POR MISHEL
    public static String encriptarContrasena(String contrasena) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(contrasena.getBytes());
        StringBuilder hexString = new StringBuilder();
        for (byte b : hash) {
            hexString.append(String.format("%02x", b));
        }
        return hexString.toString();  // Retorna la contraseña encriptada en formato hexadecimal
    }
}
