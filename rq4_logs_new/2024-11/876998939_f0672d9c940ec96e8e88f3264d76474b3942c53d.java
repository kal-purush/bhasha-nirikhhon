package com.smartverse.churchlitebackend.handlers.planaccount;

import com.smartverse.churchlitebackend_gen.PlanAccountDTO;
import com.smartverse.churchlitebackend_gen.PlanAccountHandler;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
public class PlanAccountHandlerImpl extends PlanAccountHandler {


    @Override
    public PlanAccountDTO update(PlanAccountDTO obj, UUID id) {
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
            repository.delete(entity);
        }
    }
}