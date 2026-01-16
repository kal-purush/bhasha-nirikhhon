package com.proyecto.integrador.hotel.libertador.controllers;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.proyecto.integrador.hotel.libertador.models.entity.Archivos;
import com.proyecto.integrador.hotel.libertador.models.entity.Categoria;
import com.proyecto.integrador.hotel.libertador.models.entity.Habitacion;
import com.proyecto.integrador.hotel.libertador.models.entity.Servicio;
import com.proyecto.integrador.hotel.libertador.models.entity.Usuario;
import com.proyecto.integrador.hotel.libertador.models.service.IArchivosService;
import com.proyecto.integrador.hotel.libertador.models.service.ICategoriaService;
import com.proyecto.integrador.hotel.libertador.models.service.IHabitacionService;
import com.proyecto.integrador.hotel.libertador.models.service.IServicioService;
import com.proyecto.integrador.hotel.libertador.models.service.IUploadFileService;
import com.proyecto.integrador.hotel.libertador.models.service.IUsuarioService;

@RestController
@RequestMapping("/api")
@CrossOrigin(origins = { "http://localhost:5173" })
public class ArchivoRestController {

	private final Logger log = LoggerFactory.getLogger(ArchivoRestController.class);

	@Autowired
	private IArchivosService archivosService;

	@Autowired
	private ICategoriaService categoriaService;

	@Autowired
	private IHabitacionService habitacionService;

	@Autowired
	private IServicioService servicioService;

	@Autowired
	private IUsuarioService usuarioService;

	@Autowired
	private IUploadFileService uploadFileService;

	@PostMapping("/archivo/subir")
	public ResponseEntity<?> subirArchivo(@RequestParam("archivo") MultipartFile archivo,
			@RequestParam("tipoEntidad") String tipoEntidad, @RequestParam("idEntidad") Long idEntidad) {

		Map<String, Object> response = new HashMap<>();
		Archivos nuevoArchivo = new Archivos();

		try {
			switch (tipoEntidad.toLowerCase()) {
			case "categoria":
				Categoria categoria = categoriaService.findById(idEntidad);
				nuevoArchivo.setCategoria(categoria);
				break;
			case "habitacion":
				Habitacion habitacion = habitacionService.findById(idEntidad);
				nuevoArchivo.setHabitacion(habitacion);
				break;
			case "servicio":
				Servicio servicio = servicioService.findById(idEntidad);
				nuevoArchivo.setServicio(servicio);
				break;
			case "usuario":
				Usuario usuario = usuarioService.findById(idEntidad);
				nuevoArchivo.setUsuario(usuario);
				break;
			default:
				response.put("error", "Tipo de entidad no válido");
				return ResponseEntity.badRequest().body(response);
			}

			if (!archivo.isEmpty()) {
				String nombreArchivo = uploadFileService.copiar(archivo);
				nuevoArchivo.setNombre(nombreArchivo);
				archivosService.save(nuevoArchivo);

				response.put("mensaje", "Archivo subido correctamente");
				response.put("nombreArchivo", nombreArchivo);
				return ResponseEntity.ok(response);
			} else {
				return ResponseEntity.badRequest().body("El archivo está vacío");
			}
		} catch (IOException e) {
			log.error("Error al subir el archivo", e);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error al subir el archivo");
		}
	}

	@GetMapping("/archivo/descargar/{nombreFoto:.+}")
	public ResponseEntity<Resource> descarArchivo(@PathVariable String nombreFoto) {
		Resource recurso = null;

		try {
			recurso = uploadFileService.cargar(nombreFoto);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}

		HttpHeaders cabecera = new HttpHeaders();
		cabecera.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + recurso.getFilename() + "\"");
		return new ResponseEntity<Resource>(recurso, cabecera, HttpStatus.OK);
	}

	@DeleteMapping("/archivo/eliminar/{nombreFoto:.+}")
	public ResponseEntity<?> eliminarArchivo(@PathVariable String nombreFoto) {
		Map<String, Object> response = new HashMap<>();

		try {
	        uploadFileService.eliminar(nombreFoto);
	        Archivos archivo = archivosService.findByNombre(nombreFoto);
	        if (archivo != null) {
	            archivosService.delete(archivo.getId());
	            response.put("mensaje", "Archivo eliminado y referencias actualizadas correctamente");
	            return ResponseEntity.ok(response);
	        } else {
	            response.put("error", "No se encontró el archivo en la entidad Archivos");
	            return new ResponseEntity<Map<String, Object>>(response, HttpStatus.NOT_FOUND);
	        }
	    } catch (Exception e) {
	        response.put("error", "Error al eliminar el archivo: " + e.getMessage());
	        return new ResponseEntity<Map<String, Object>>(response, HttpStatus.INTERNAL_SERVER_ERROR);
	    }
	}
}