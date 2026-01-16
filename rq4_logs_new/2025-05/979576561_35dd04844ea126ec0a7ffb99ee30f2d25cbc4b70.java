package software.sebastian.oposiciones.controller;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import software.sebastian.oposiciones.service.EtiquetaService;

@Controller
@RequestMapping("/etiquetas")
public class EtiquetaController {

    private final EtiquetaService service;

    public EtiquetaController(EtiquetaService svc) {
        this.service = svc;
    }

    @GetMapping
    public String list(Model model) {
        model.addAttribute("etiquetas", service.findAll());
        return "etiquetas";
    }

    @PostMapping
    public String create(
        @RequestParam String nombre,
        @RequestParam(required = false) String descripcion
    ) {
        service.create(nombre, descripcion);
        return "redirect:/etiquetas";
    }

    @PostMapping("/{id}/edit")
    public String edit(
        @PathVariable Integer id,
        @RequestParam String nombre,
        @RequestParam(required = false) String descripcion
    ) {
        service.update(id, nombre, descripcion);
        return "redirect:/etiquetas";
    }

    @PostMapping("/{id}/delete")
    public String delete(@PathVariable Integer id) {
        service.delete(id);
        return "redirect:/etiquetas";
    }

    @PostMapping("/relacion")
    public String relacion(
        @RequestParam Integer parentId,
        @RequestParam Integer childId
    ) {
        service.addRelation(parentId, childId);
        return "redirect:/etiquetas";
    }
}