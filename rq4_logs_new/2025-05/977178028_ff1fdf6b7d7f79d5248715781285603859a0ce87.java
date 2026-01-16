package um.programacion2.prestamo;

import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import java.net.URI;
import java.util.List;

@RestController
@RequestMapping("/api/prestamos")
public class PrestamoController {
    private final PrestamoService prestamosService;

    public PrestamoController(PrestamoService prestamosService) {
        this.prestamosService = prestamosService;
    }

    @GetMapping
    public ResponseEntity<List<Prestamo>> obtenerTodos() {
        List<Prestamo> libros = prestamosService.listarPrestamos();
        return ResponseEntity.ok(libros);
    }

    @GetMapping("/{id}")
    public ResponseEntity<Prestamo> obtenerPorId(@PathVariable Long id) {
        Prestamo libro = prestamosService.buscarPorId(id);
        return ResponseEntity.ok(libro);
    }

    @PostMapping
    public ResponseEntity<Void> crear(@Valid @RequestBody Prestamo libro) {
        prestamosService.guardar(libro);

        // Crear URI del recurso creado
        URI location = ServletUriComponentsBuilder
                .fromCurrentRequest()
                .path("/{id}")
                .buildAndExpand(libro.getId())
                .toUri();

        // Devolver 201 Created con la ubicación del recurso
        return ResponseEntity.created(location).build();
    }

    @PutMapping("/{id}")
    public ResponseEntity<Void> actualizar(@PathVariable Long id, @Valid @RequestBody Prestamo libro) {
        prestamosService.actualizar(id, libro);

        // Devolver 204 No Content para indicar éxito sin contenido en respuesta
        return ResponseEntity.noContent().build();
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> eliminar(@PathVariable Long id) {
        prestamosService.eliminar(id);

        // Devolver 204 No Content
        return ResponseEntity.noContent().build();
    }
}