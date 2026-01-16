package com.example.cargo_transportation.dto.mapper;

import jakarta.persistence.Entity;
import lombok.extern.log4j.Log4j2;
import org.modelmapper.ModelMapper;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Locale;

@Log4j2
public class CustomMapper implements Mapper{

    private ModelMapper modelMapper;

    public CustomMapper(ModelMapper modelMapper) {
        this.modelMapper = modelMapper;
    }

    @Override
    public <D> D map(Object source, Class<D> destinationType) {
        return modelMapper.map(source, destinationType);
    }

    @Override
    public <D> D mapWithSpecificFields(Object source, Class<D> destinationType) {
        D d = modelMapper.map(source, destinationType);
        setSpecificFields(source, d);
        return d;
    }

    private <D> void setSpecificFields(Object source, D d) {
        if (d.getClass().getAnnotation(Entity.class) == null) {
            Arrays.stream(d.getClass().getDeclaredFields())
                .filter(field -> {
                    try {
                        if (field.getName().toLowerCase(Locale.ROOT).contains("id") &&
                                !field.getName().toLowerCase(Locale.ROOT).equals("id")) {
                            field.setAccessible(true);
                            return field.get(d) == null;
                        }
                        return false;
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                        return false;
                    }
                })
                .forEach(field -> {
                    Object model;
                    String searchName = field.getName().toLowerCase(Locale.ROOT).replace("id", "");
                    try {
                        Field fieldModel = source.getClass().getDeclaredField(searchName);
                        fieldModel.setAccessible(true);
                        model = fieldModel.get(source);
                        if (model != null) {
                            Field searchField = d.getClass().getDeclaredField(field.getName());
                            searchField.setAccessible(true);
                            searchField.set(d, model.getClass().getDeclaredMethod("getId").invoke(model));
                        }
                    } catch (NoSuchFieldException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                        log.error("An error occurred searching for specific fields: " + e.getLocalizedMessage(), e);
                    }
                });
        }
    }
}