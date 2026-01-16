package um.programacion2.libro;

import org.springframework.stereotype.Repository;

import java.util.*;

@Repository
public class LibroRepositoryImpl implements LibroRepository {
    private final Map<Long, Libro> libros = new HashMap<>();
    private Long nextId = 1L;

    @Override
    public Libro save(Libro libro) {
        if (libro.getId() == null) {
            libro.setId(nextId++);
        }
        libros.put(libro.getId(), libro);
        return libro;
    }

    @Override
    public Optional<Libro> findById(Long id) {
        return Optional.ofNullable(libros.get(id));
    }

    @Override
    public Optional<Libro> findByIsbn(String isbn) {
        return libros.values().stream()
                .filter(libro -> libro.getIsbn().equals(isbn))
                .findFirst();
    }

    @Override
    public List<Libro> findAll() {
        return new ArrayList<>(libros.values());
    }

    @Override
    public void deleteById(Long id) {
        libros.remove(id);
    }

    @Override
    public boolean existsById(Long id) {
        return libros.containsKey(id);
    }
}