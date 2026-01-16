import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamSource;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

public class Main {
        public static void main(String[] args) throws Exception {
            TransformerFactory tf = TransformerFactory.newInstance();
    
            tf.setAttribute("generate-translet", true);
            tf.setAttribute("translet-name", "MyTranslet");
            tf.setAttribute("package-name", "org.acme");
            tf.setAttribute("destination-directory", "test");
    
            Files.walk(Path.of("test")).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);

            File xslFile = new File(args[0]);
            tf.newTemplates(new StreamSource(Files.newInputStream(xslFile.toPath())));

            Files.list(Path.of("test/org/acme")).forEach(System.out::println);
        }
}