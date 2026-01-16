package org.openapitools.controllers;


import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.openapitools.dto.NotificacionDTO.NotificacionRequest;
import org.openapitools.dto.NotificacionDTO.NotificacionResponse;
import org.openapitools.dto.SuccessResponse;
import org.openapitools.dto.UsuarioDTO.UsuarioResponse;
import org.openapitools.services.interfaces.NotificacionService;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/notificaciones")
@RequiredArgsConstructor
public class NotificacionController {

    private final NotificacionService notificacionService;

    @PostMapping("notificaciones")
    public ResponseEntity<SuccessResponse> sendNotification(UsuarioResponse usuario,@Valid @RequestBody NotificacionRequest notificacion) {
        var respuesta = notificacionService.sendNotificacion(usuario.id(), notificacion);
        return respuesta.map(ResponseEntity::ok).orElseGet(() -> ResponseEntity.notFound().build());
    }
}