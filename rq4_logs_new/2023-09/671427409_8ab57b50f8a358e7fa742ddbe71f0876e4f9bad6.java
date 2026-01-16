package fr.digi.absences.controller;


import fr.digi.absences.dto.ListAbsByEmployeeDto;
import fr.digi.absences.service.DemandeSrvc;
import fr.digi.absences.service.JwtService;
import jakarta.annotation.security.RolesAllowed;
import lombok.AllArgsConstructor;
import org.springframework.context.annotation.Role;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@AllArgsConstructor
@RequestMapping("/api/v1/datahistogram")
public class ListAbsByEmployeeCtrl {

    private DemandeSrvc demandeSrvc;
    private JwtService jwtService;

    @RolesAllowed("MANAGER")
    @GetMapping("/{id}")
    public ResponseEntity<ListAbsByEmployeeDto> displayAbsByEmployee(@CookieValue("AUTH-TOKEN") String token, @PathVariable long id){
            String email = jwtService.extractEmail(token);
            return ResponseEntity.status(200).body(demandeSrvc.getMonthAbsence(id, email));
    }

}