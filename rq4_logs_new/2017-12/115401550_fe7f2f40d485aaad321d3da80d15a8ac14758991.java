package com.heavenhr.api.utils;

import org.springframework.context.support.DefaultMessageSourceResolvable;
import org.springframework.validation.BindingResult;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Author: Kingsley Eze.
 * Project: heavenhr
 * Date: 12/27/2017.
 */
public class ErrorsUtils {

    public static List<String> getErrors(BindingResult result){

        return result.getAllErrors().stream()
                .map(DefaultMessageSourceResolvable::getDefaultMessage)
                .collect(Collectors.toList());
    }
}