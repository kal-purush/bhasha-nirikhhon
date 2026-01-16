package uz.pdp.simline.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import uz.pdp.simline.dto.respone.PlanDto;
import uz.pdp.simline.service.PlanService;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/plan")
@RequiredArgsConstructor
public class PlanController {
    private final PlanService planService;
    @PatchMapping("/create")
    public ResponseEntity<PlanDto> create(@RequestBody PlanDto planDto) {
        return ResponseEntity.ok(planService.createPlan(planDto));
    }
    @PutMapping("/update")
    public ResponseEntity<PlanDto> update(@RequestBody PlanDto planDto) {
        return ResponseEntity.ok(planService.updatePlan(planDto));
    }

    @DeleteMapping("/delete/{id}")
    public void deletePlan(@PathVariable UUID id) {
        planService.deletePlan(id);
    }

    @DeleteMapping("/delete/name/{name}")
    public void deletePlanByName(@PathVariable String name) {
        planService.deletePlanByName(name);
    }

    @GetMapping("/{id}")
    public ResponseEntity<PlanDto> getPlanById(@PathVariable UUID id) {
        return ResponseEntity.ok(planService.getPlanById(id));
    }

    @GetMapping("/all")
    public ResponseEntity<List<PlanDto>> getAllPlans() {
        return ResponseEntity.ok(planService.getAll());
    }

    @GetMapping("/name/{name}")
    public ResponseEntity<PlanDto> getPlanByName(@PathVariable String name) {
        return ResponseEntity.ok(planService.getPlanByName(name));
    }

    @GetMapping("/price/{price}")
    public ResponseEntity<PlanDto> getPlanByPrice(@PathVariable Double price) {
        return ResponseEntity.ok(planService.getPlanByPrice(price));
    }

    @GetMapping("/price/less/{price}")
    public ResponseEntity<List<PlanDto>> getPlansWithPriceLessThan(@PathVariable Double price) {
        return ResponseEntity.ok(planService.getPlansWithPriceLessThan(price));
    }
}