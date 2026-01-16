package software.sebastian.oposiciones.controller;

import java.util.List;
import java.security.Principal;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import software.sebastian.oposiciones.dto.SuscripcionForm;
import software.sebastian.oposiciones.model.Etiqueta;
import software.sebastian.oposiciones.model.Suscripcion;
import software.sebastian.oposiciones.model.Usuario;
import software.sebastian.oposiciones.service.EtiquetaService;
import software.sebastian.oposiciones.service.SuscripcionService;
import software.sebastian.oposiciones.service.SuscripcionEtiquetaService;
import software.sebastian.oposiciones.service.UsuarioService;

@Controller
public class SuscripcionController {

    private final SuscripcionService service;
    private final SuscripcionEtiquetaService serviceSE;

    private final UsuarioService usService;
    private final EtiquetaService etiquetaService;

    public SuscripcionController(SuscripcionService service, SuscripcionEtiquetaService serviceSE,
            UsuarioService usService, EtiquetaService etiquetaService) {
        this.service = service;
        this.serviceSE = serviceSE;
        this.usService = usService;
        this.etiquetaService = etiquetaService;
    }

    @GetMapping("/suscripciones")
    public String verSuscripciones(Model model, Principal principal) {

        Usuario usuario = usService.getCurrentUser(principal); 
        model.addAttribute("suscripciones_usuario", service.findSusByUser(usuario.getUsuarioId()));
        model.addAttribute("etiquetasPorSuscripcion", serviceSE.getEtiquetasPorSuscripcion());
        return "suscripciones/suscripciones";
    }

    @GetMapping("/suscripciones/nueva_sus")
    public String nuevaSuscripcion(Model model) {
        List<Etiqueta> etiquetas = etiquetaService.findAll();
        model.addAttribute("etiquetas", etiquetas);
        model.addAttribute("suscripcionForm", new SuscripcionForm());
          model.addAttribute("actionUrl", "/suscripciones/guardar");
     
        return "suscripciones/nueva_sus";
    }

    @PostMapping("/suscripciones/guardar")
    public String guardarSuscripcion(@ModelAttribute SuscripcionForm form, Principal principal) {
         Usuario usuario = usService.getCurrentUser(principal); 
        service.create(form.getEtiquetasSeleccionadas(), usuario.getUsuarioId());
        return "redirect:/suscripciones";
    }

    @PostMapping("/suscripciones/eliminar/{id}")
    public String eliminarSuscripcion(@PathVariable Integer id) {
        serviceSE.deleteEtiquetasPorSuscripcion(id);
        service.delete(id);
        return "redirect:/suscripciones?success=Suscripción+eliminada";
    }



    @GetMapping("/suscripciones/editar/{id}")
    public String mostrarFormularioEdicion(@PathVariable Integer id, Model model) {
        Suscripcion suscripcion = service.findById(id);
        model.addAttribute("suscripcion", suscripcion);
        List<Etiqueta> etiquetas = etiquetaService.findAll();
        model.addAttribute("etiquetas", etiquetas);
        SuscripcionForm nuestralista = new SuscripcionForm();
        nuestralista.setEtiquetasSeleccionadas(serviceSE.getEtiquetaIdsPorSuscripcion(suscripcion.getSuscripcionId()));
        model.addAttribute("suscripcionForm", nuestralista);
           model.addAttribute("actionUrl", "/suscripciones/editar/" + id + "/guardar");
        return "suscripciones/nueva_sus";
    }

    @PostMapping("/suscripciones/editar/{id}/guardar")
    public String procesarEdicion(@PathVariable Integer id, @ModelAttribute SuscripcionForm form) {
        serviceSE.update(form.getEtiquetasSeleccionadas(),id);
        return "redirect:/suscripciones";
    }

 
}