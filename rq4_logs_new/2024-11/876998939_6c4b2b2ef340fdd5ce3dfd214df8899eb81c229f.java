package com.smartverse.churchlitebackend.handlers.costcenter;

import com.smartverse.churchlitebackend_gen.CostCenterDTO;
import com.smartverse.churchlitebackend_gen.CostCenterHandler;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
public class CostCenterHandlerImpl extends CostCenterHandler {

    @Override
    public CostCenterDTO update(CostCenterDTO obj, UUID id) {
        var entity = dtoConverter.toEntity(obj, null);
        entity.setId(id);
        var hasChildren = repository.findById(id).orElse(null);
        if(hasChildren != null){
            entity.setChildren(hasChildren.getChildren());
        }
        entityManager.merge(entity);
        return dtoConverter.toDTO(entity, null);
    }

    @Override
    public void delete(UUID id) {
        var entity = repository.findById(id).orElse(null);
        if(entity != null){
            if(entity.getParentCode() != null){
                var parent = repository.findById(entity.getParentCode().getId()).orElse(null);
                parent.getChildren().remove(entity);
                entityManager.merge(parent);
            } else {
                entityManager.remove(entity);
            }
        }
    }
}