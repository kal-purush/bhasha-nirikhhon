package com.ssm.validate;


import com.ssm.entity.UserInfo;
import org.apache.commons.lang3.StringUtils;
import org.springframework.validation.Errors;
import org.springframework.validation.ValidationUtils;
import org.springframework.validation.Validator;


public class ParamValidate implements Validator {
    @Override
    public boolean supports(Class<?> clazz) {
        return false;
    }

    @Override
    public void validate(Object target, Errors errors) {
        ValidationUtils.rejectIfEmpty(errors,"username",null,"姓名不能为空!");
        UserInfo userInfo = (UserInfo) target;
        if(StringUtils.isEmpty(userInfo.getUserpassword())){
            errors.rejectValue("userpassword",null,"密码不能为空");
        }
    }
}

