package com.mggcode.gestion_bd_elecciones.controller.municipales.vistas;


import com.mggcode.gestion_bd_elecciones.controller.municipales.CircunscripcionController;
import com.mggcode.gestion_bd_elecciones.controller.municipales.CircunscripcionPartidoController;
import com.mggcode.gestion_bd_elecciones.controller.municipales.PartidoController;
import com.mggcode.gestion_bd_elecciones.model.municipales.Circunscripcion;
import com.mggcode.gestion_bd_elecciones.model.municipales.CircunscripcionPartido;
import com.mggcode.gestion_bd_elecciones.model.municipales.Partido;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.List;

@Controller
@RequestMapping("/municipales")
public class VistasController {
    @Autowired
    private CircunscripcionController circunscripcionController;
    @Autowired
    private PartidoController partidoController;
    @Autowired
    private CircunscripcionPartidoController cpController;

    @GetMapping("/circunscripciones/vista")
    public String verCircunscripciones(Model model) {
        List<Circunscripcion> circunscripciones = circunscripcionController.findAll().getBody();
        model.addAttribute("circunscripciones", circunscripciones);
        model.addAttribute("rutaCSV", "/municipales/circunscripciones/csv");
        model.addAttribute("rutaExcel", "/municipales/circunscripciones/excel");
        return "circunscripciones";
    }

    @GetMapping("/partidos/vista")
    public String verPartidos(Model model) {
        List<Partido> partidos = partidoController.findAll().getBody();
        model.addAttribute("partidos", partidos);
        model.addAttribute("rutaCSV", "/municipales/partidos/csv");
        model.addAttribute("rutaExcel", "/municipales/partidos/excel");
        return "partidos";
    }

    @GetMapping("/cp/vista")
    public String verCPS(Model model) {
        List<CircunscripcionPartido> cps = cpController.findAll().getBody();
        model.addAttribute("cps", cps);
        model.addAttribute("rutaCSV", "/municipales/cp/csv");
        model.addAttribute("rutaExcel", "/municipales/cp/excel");
        return "cps";
    }

    @GetMapping("/cp/mayorias/autonomias/vista")
    public String verCPSporMA(Model model) {
        List<CircunscripcionPartido> cps = cpController.masVotadosPorAutonomia().getBody();
        model.addAttribute("cps", cps);
        model.addAttribute("rutaCSV", "/municipales/cp/mayorias/autonomias/csv");
        model.addAttribute("rutaExcel", "/municipales/cp/mayorias/autonomias/excel");
        return "cps";
    }

    @GetMapping("/cp/mayorias/provincias/vista")
    public String verCPSporMP(Model model) {
        List<CircunscripcionPartido> cps = cpController.masVotadosPorProvincia().getBody();
        model.addAttribute("cps", cps);
        model.addAttribute("rutaCSV", "/municipales/cp/mayorias/provincias/csv");
        model.addAttribute("rutaExcel", "/municipales/cp/mayorias/provincias/excel");
        return "cps";
    }

    @GetMapping("/cp/mayorias/{codigo}/vista")
    public String verMayoriasmunicipales(@PathVariable("codigo") String cod, Model model) {
        List<CircunscripcionPartido> cps = cpController.masVotadosAutonomicoPorProvincia(cod).getBody();
        model.addAttribute("cps", cps);
        model.addAttribute("rutaCSV", "/municipales/cp/mayorias/" + cod + "/csv");
        model.addAttribute("rutaExcel", "/municipales/cp/mayorias/" + cod + "/excel");
        return "cps";
    }

    @GetMapping("/cp/circunscripcion/{codigo}/vista")
    public String verTodosCPDeUnaCircunscripcion(@PathVariable("codigo") String cod, Model model) {
        List<CircunscripcionPartido> cps = cpController.findByIdCircunscripcion(cod).getBody();
        model.addAttribute("cps", cps);
        model.addAttribute("rutaCSV", "/municipales/cp/circunscripcion/" + cod + "/csv");
        model.addAttribute("rutaExcel", "/municipales/cp/circunscripcion/" + cod + "/excel");
        return "cps";
    }

    @GetMapping("/cp/partido/{codigo}/autonomias/orderByCodAuto/vista")
    public String verTodoDePartidoOrderAuto(@PathVariable("codigo") String cod, Model model) {
        List<CircunscripcionPartido> cps = cpController.findByIdPartidoAutonomiasCod(cod).getBody();
        model.addAttribute("cps", cps);
        model.addAttribute("rutaCSV", "/municipales/cp/partido/" + cod + "/autonomias/orderByCodAuto/csv");
        model.addAttribute("rutaExcel", "/municipales/cp/partido/" + cod + "/autonomias/orderByCodAuto/excel");
        return "cps";
    }

    @GetMapping("/cp/partido/{codigo}/autonomias/orderByEscanios/vista")
    public String verTodoDePartidoOrderEscanios(@PathVariable("codigo") String cod, Model model) {
        List<CircunscripcionPartido> cps = cpController.findByIdPartidoAutonomiasEscanios(cod).getBody();
        model.addAttribute("cps", cps);
        model.addAttribute("rutaCSV", "/municipales/cp/partido/" + cod + "/autonomias/orderByEscanios/csv");
        model.addAttribute("rutaExcel", "/municipales/cp/partido/" + cod + "/autonomias/orderByEscanios/excel");
        return "cps";
    }

    @GetMapping("/cp/partido/{codigo}/provincias/vista")
    public String verTodoDePartidoPorProvincias(@PathVariable("codigo") String cod, Model model) {
        List<CircunscripcionPartido> cps = cpController.findByIdPartidoProvincias(cod).getBody();
        model.addAttribute("cps", cps);
        model.addAttribute("rutaCSV", "/municipales/cp/partido/" + cod + "/provincias/csv");
        model.addAttribute("rutaExcel", "/municipales/cp/partido/" + cod + "/provincias/excel");
        return "cps";
    }
}