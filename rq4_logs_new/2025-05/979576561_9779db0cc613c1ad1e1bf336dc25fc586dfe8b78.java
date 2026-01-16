package software.sebastian.oposiciones.controller.user;

import java.util.List;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import software.sebastian.oposiciones.service.EtiquetaService;
import software.sebastian.oposiciones.service.EtiquetaService.TreeNode;

@Controller
@RequestMapping("/user/etiquetas")
public class EtiquetaUserController {

    private final EtiquetaService svc;
    public EtiquetaUserController(EtiquetaService svc) { this.svc = svc; }

    @GetMapping
    public String tree(Model m) {
        List<TreeNode> forest = svc.getTree();
        m.addAttribute("forest", forest);
        return "etiquetas/tree";
    }
}