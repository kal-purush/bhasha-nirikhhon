package ru.nsu.peretyatko.controller.equipments;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.*;
import ru.nsu.peretyatko.dto.equipments.EquipmentCategoryRequest;
import ru.nsu.peretyatko.dto.equipments.EquipmentCategoryResponse;
import ru.nsu.peretyatko.service.equipments.EquipmentCategoryService;
import ru.nsu.peretyatko.validator.equipments.EquipmentCategoryValidator;

import java.util.List;

@RestController
@RequestMapping("/api/equipment/category")
@RequiredArgsConstructor
public class EquipmentCategoryController {

    private final EquipmentCategoryService equipmentCategoryService;

    private final EquipmentCategoryValidator equipmentCategoryValidator;

    @GetMapping
    public List<EquipmentCategoryResponse> getEquipmentCategories() {
        return equipmentCategoryService.getEquipmentCategories();
    }

    @GetMapping("/{id}")
    public EquipmentCategoryResponse getEquipmentCategoryById(@PathVariable int id) {
        return equipmentCategoryService.getEquipmentCategory(id);
    }

    @PostMapping
    public void createEquipmentCategory(@Valid @RequestBody EquipmentCategoryRequest equipmentCategoryRequest,
                                   BindingResult bindingResult) {
        equipmentCategoryValidator.validate(equipmentCategoryRequest, bindingResult);
        equipmentCategoryService.createEquipmentCategory(equipmentCategoryRequest);
    }

    @DeleteMapping("/{id}")
    public void deleteEquipmentCategory(@PathVariable int id) {
        equipmentCategoryService.deleteEquipmentCategory(id);
    }
}