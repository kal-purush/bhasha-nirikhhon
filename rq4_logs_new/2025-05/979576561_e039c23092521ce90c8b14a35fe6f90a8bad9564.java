package software.sebastian.oposiciones.controller;

import org.springframework.web.bind.annotation.*;
import software.sebastian.oposiciones.repository.UsuarioRepository;

@RestController
@RequestMapping("/api/usuarios")
public class UsuarioRestController {

    private final UsuarioRepository userRepo;

    public UsuarioRestController(UsuarioRepository userRepo) {
        this.userRepo = userRepo;
    }

    @GetMapping("/exists")
    public boolean emailExists(@RequestParam String email) {
        return userRepo.findByEmail(email).isPresent();
    }
}