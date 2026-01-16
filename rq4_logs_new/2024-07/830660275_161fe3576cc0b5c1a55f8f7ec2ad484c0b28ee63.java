package com.eroseloi.todolist.utils;

import java.beans.PropertyDescriptor;
import java.util.HashSet;
import java.util.Set;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;

import ch.qos.logback.core.joran.util.beans.BeanUtil;

public class Utils {

    // Método que copia propriedades não nulas de um objeto fonte para um objeto
    // alvo.
    public static void copyNonNullPropeties(Object source, Object target) {

        // Copia as propriedades do 'source' para o 'target', ignorando as propriedades
        // que são nulas no 'source'
        BeanUtils.copyProperties(source, target, getNullPropetyNames(source));
    }

    // Método auxiliar que retorna um array com os nomes das propriedades que são
    // nulas no objeto fonte.
    public static String[] getNullPropetyNames(Object source) {

        // Cria um BeanWrapper para o objeto fonte, que permite acessar propriedades do
        // bean.
        final BeanWrapper src = new BeanWrapperImpl(source);

        // Obtém descritores das propriedades do objeto fonte.
        PropertyDescriptor[] pds = src.getPropertyDescriptors();

        // Conjunto para armazenar os nomes das propriedades que são nulas.
        Set<String> emptyNames = new HashSet<>();

        // Percorre todas as propriedades do objeto fonte.
        for (PropertyDescriptor pd : pds) {

            // Obtém o valor da propriedade atual.
            Object srcValue = src.getPropertyValue(pd.getName());

            // Se o valor da propriedade for nulo, adiciona o nome da propriedade ao
            // conjunto.
            if (srcValue == null) {
                emptyNames.add(pd.getName());
            }

        }

        // Converte o conjunto de nomes de propriedades nulas para um array de strings
        String[] result = new String[emptyNames.size()];
        return emptyNames.toArray(result);
    }

}