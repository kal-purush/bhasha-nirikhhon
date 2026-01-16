package uz.pdp.simline.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import uz.pdp.simline.dto.request.SimCardUpdateDto;
import uz.pdp.simline.service.SimCardService;

import java.util.UUID;

@RestController
@RequestMapping("/sim-card")
@RequiredArgsConstructor
public class SimCardController {
    private final SimCardService simCardService;

    @GetMapping("/{id}")
    public ResponseEntity<?> getById(@PathVariable UUID id){
        return ResponseEntity.ok(simCardService.getById(id));
    }
    @GetMapping("/number/{number}")
    public ResponseEntity<?> getByNumber(@PathVariable String number){
        return ResponseEntity.ok(simCardService.getByNumber(number));
    }
    @GetMapping("/price/{minPrice}/{maxPrice}")
    public ResponseEntity<?> getByPrice(@PathVariable Double minPrice, @PathVariable Double maxPrice){
        return ResponseEntity.ok(simCardService.getByPrice(minPrice, maxPrice));
    }
    @GetMapping("/activity/{isActive}")
    public ResponseEntity<?> getByActivity(@PathVariable Boolean isActive){
        return ResponseEntity.ok(simCardService.getByActivity(isActive));
    }
    @PutMapping("/update")
    public ResponseEntity<?> update(@RequestBody SimCardUpdateDto simCardUpdateDto){
        return ResponseEntity.ok(simCardService.update(simCardUpdateDto));
    }
    @GetMapping("/allByPlanId/{planId}")
    public ResponseEntity<?> getAllByPlan(@PathVariable UUID planId){
        return ResponseEntity.ok(simCardService.getAllByPlanId(planId));
    }
    @GetMapping("/all")
    public ResponseEntity<?> getAll(){
        return ResponseEntity.ok(simCardService.getAll());
    }



}