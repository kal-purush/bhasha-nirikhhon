package com.marketplace.annotations.process;

import com.squareup.javapoet.ClassName;

import javax.lang.model.type.TypeMirror;

public abstract class AbstractCodeBuilder {
    protected ClassName getName(TypeMirror typeMirror) {
        String rawString = typeMirror.toString();
        int dotPosition = rawString.lastIndexOf(".");
        String packageName = rawString.substring(0, dotPosition);
        String className = rawString.substring(dotPosition + 1);
        return ClassName.get(packageName, className);
    }
}