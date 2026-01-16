/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/UnitTests/JUnit5TestClass.java to edit this template
 */
package grupo2.ut9_ta4;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 *
 * @author santi
 */
public class TClasificadorTest {
    
    private GeneradorDatosGenericos generador = new GeneradorDatosGenericos();
    
    public TClasificadorTest() {
    }

    @Test
    public void testOrdenarPorQuickSort300() {
        int[] vector = generador.generarDatosAleatorios();
        TClasificador clasificador = new TClasificador();
    }
    
}