package dyasc.fibonacci;

import java.io.IOException;

public interface AplicarOpcion {
    public Salida aplicar(Salida salida, String detalle ) throws IOException;
}