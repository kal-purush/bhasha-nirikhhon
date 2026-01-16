package jp.dodododo.dao.exception;

import jp.dodododo.dao.message.Message;

import java.lang.reflect.Modifier;

public class InstantiationRuntimeException extends RuntimeException {
    private Class<?> targetClass;

    public InstantiationRuntimeException(Class<?> targetClass) {
        super(Message.getMessage("00056", targetClass.getName(), getType(targetClass)));
        this.targetClass = targetClass;
    }

    private static String getType(Class<?> clazz) {
        String ret = null;
        int modifiers = clazz.getModifiers();
        if (Modifier.isInterface(modifiers)) {
            ret = "interface";
        } else if (Modifier.isAbstract(modifiers))
            ret = "abstract";
        return ret;
    }

    public Class<?> getTargetClass() {
        return targetClass;
    }
}