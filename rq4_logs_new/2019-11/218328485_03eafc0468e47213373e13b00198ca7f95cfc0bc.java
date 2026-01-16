package com.project.teamup.service.validator;

import com.project.teamup.model.User;
import com.project.teamup.model.UserSkill;
import com.project.teamup.model.UserSkillLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component
public class UserSkillsValidator {
    @Autowired
    private TechnologyValidator technologyValidator;

    public boolean validateObject(List<UserSkill> userSkillList){
        if (userSkillList != null && userSkillList.size() != 0){
            for (UserSkill userSkill : userSkillList){
                if (userSkill.getUser() == null) return false;
                if (!technologyValidator.validateObject(userSkill.getTechnology())) return false;
                List<UserSkillLevel> userSkillLevelList = Arrays.asList(UserSkillLevel.values());
                if (!userSkillLevelList.contains(userSkill.getLevel())) return false;
            }
            return true;
        }
        return true;
    }
}