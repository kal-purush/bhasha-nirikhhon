package com.delta.deltanet.controllers;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.delta.deltanet.models.entity.CatalogoServicio;
import com.delta.deltanet.models.entity.UsuarioServicio;
import com.delta.deltanet.models.service.ICatalogoServicioService;
import com.delta.deltanet.models.service.IUsuarioServicioService;

@RestController
@RequestMapping("/ticket/CatalogoUsuarios/")
public class TicketCatalogoUsuariosController {
	@Autowired
	private IUsuarioServicioService usuarioServicioService;
	@Autowired
	private ICatalogoServicioService catalogoServicioService;
	
	@PostMapping("/Create")
	public ResponseEntity<?> creaUsuario(@RequestParam("idUsuario") Long idUsuario,
			                             @RequestParam("idCatalogos[]") List<Long> idCatalogos,
			                             @RequestParam("userCrea") String userCrea
			                            ){
		Map<String, Object> response = new HashMap<>();
		UsuarioServicio usuario = null;
		List<CatalogoServicio> catalogos = new ArrayList<CatalogoServicio>();
		CatalogoServicio catalogo = null;
		System.out.println("Inicio");
		try {
			usuario = usuarioServicioService.findByIdAndEstado(idUsuario, "A");
			System.out.println("Inicio 1");
			if (usuario == null) {
				response.put("mensaje", "Usuario no encontrado o habilitado");
				return new ResponseEntity<Map<String,Object>>(response,HttpStatus.NOT_FOUND);
			}
		} catch (Exception e) {
			response.put("mensaje", "Error al buscar usuario.");
			response.put("Error", e.getMessage());
			return new ResponseEntity<Map<String,Object>>(response,HttpStatus.NOT_FOUND);
		}
		System.out.println("PAso");
		
		for(int i=0; i < idCatalogos.size(); i++) {
			catalogo = catalogoServicioService.findByIdAndEstado(idCatalogos.get(i), "A");
			System.out.println("Inicio 2");
			if (catalogo==null) {
				response.put("mensaje", "Catalogo no encontrado o habilitado");
				return new ResponseEntity<Map<String,Object>>(response,HttpStatus.NOT_FOUND);
			}
			System.out.println("Agrega a la matriz");
			catalogos.add(catalogo);
			
		}
		System.out.println(catalogos);
		
		try {
			System.out.println("Inicia actualizacion");
			usuario.setCatalogoServicios(catalogos);
			usuario.setUsuEditado(userCrea);
			usuario.setFechaEditado(new Date());
			usuarioServicioService.save(usuario);
			System.out.println("Inicio 3");
			response.put("Mensaje", "Se agregaron los catalogos requeridos al usuario.");
			return new ResponseEntity<Map<String,Object>>(response,HttpStatus.OK);
		} catch (Exception e) {
			response.put("Mensaje", "Error al agregar catalogos requeridos al usuario.");
			response.put("Error", e.getMessage());
			return new ResponseEntity<Map<String,Object>>(response,HttpStatus.INTERNAL_SERVER_ERROR);
		}

	}
	
	@GetMapping("/ReadUsuario")
	public ResponseEntity<?> ListByUsuario(@RequestParam(value = "id", required=false) Long idUsuario
			                            ){
		Map<String, Object> response = new HashMap<>();
		List<CatalogoServicio> catalogos = new ArrayList<CatalogoServicio>();
		UsuarioServicio usuario = null;
		if (idUsuario == null) {
			
		}else {
			usuario = usuarioServicioService.findByIdAndEstado(idUsuario, "A");
		}
		System.out.println(idUsuario);
		response.put("Mensaje", "Se listaron los catalogos requeridos al usuario.");
		return new ResponseEntity<Map<String,Object>>(response,HttpStatus.OK);
	}

}