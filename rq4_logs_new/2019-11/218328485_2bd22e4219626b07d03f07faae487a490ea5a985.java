package com.project.teamup.service.validator;

import com.project.teamup.model.TechnologyArea;
import org.springframework.stereotype.Component;

/**
 * Validator class for {@link TechnologyArea}.
 * @author Sebastian
 */
@Component
public class TechnologyAreaValidator {

    public boolean validateObject(TechnologyArea technologyArea){
        if (technologyArea == null) return false;
        if (technologyArea.getName().isEmpty()) return false;
        return technologyArea.getName().length() <= 50;
    }
}