package sptech.school.projetovolt.api.util;

import org.springframework.http.ResponseEntity;

import java.net.URI;
import java.util.List;

public class ResponseUtil {

    public static <T> ResponseEntity<T> respondIfNotNull(T entity) {
        if (entity == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(entity);
    }

    public static <T> ResponseEntity<List<T>> respondIfNotEmpty(List<T> entities) {
        if (entities.isEmpty()) {
            return ResponseEntity.noContent().build();
        }
        return ResponseEntity.ok(entities);
    }

    public static <T> ResponseEntity<T> respondCreated(T entity, String path, int id) {
        URI uri = URI.create(path + "/" + id);
        return ResponseEntity.created(uri).body(entity);
    }
}