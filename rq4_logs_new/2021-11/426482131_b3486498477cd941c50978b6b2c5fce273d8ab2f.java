package co.edu.javeriana.pica.front.core.interfaces;

import java.io.File;
import java.util.Optional;

public interface ResolucionRepository {
    Optional<File> download(String id);
}