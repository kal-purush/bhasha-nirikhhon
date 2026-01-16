import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class EjemploAsignarPropiedadesDeSistemas {
    public static void main(String[] args) {
        try {
            FileInputStream archivo = new FileInputStream("src/config.properties");
            Properties p = new Properties(System.getProperties());
            p.load(archivo);
            p.setProperty("mi.propiedad", "valor");

            System.setProperties(p);
            Properties ps = System.getProperties();
            System.out.println("ps.getProperty(\"mi.propiedad\") = " + ps.getProperty("mi.propiedad"));
            System.out.println(System.getProperty("otra"));
            ps.list(System.out);

        }catch (IOException a){
            a.printStackTrace();
        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }
}