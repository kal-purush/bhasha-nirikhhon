package org.bluelight.lib.efficient.utils;

import org.bluelight.lib.efficient.constraint.NotNull;

/**
 * more object utils than Objects and MoreObjects.
 * Created by mikes on 15/8/30.
 */
public class ObjectPlus {
    public static <T> void requireNotAllNull(T... objs){
        if (allNull(objs)) {
            throw new NullPointerException("all args are null");
        }
    }
    public static <T> void requireAllNonNull(T... objs){
        for (int i=0; i<objs.length; i++){
            if (objs[i]==null){
                throw new NullPointerException("find null in args of index "+i);
            }
        }
    }
    public static <T> boolean allNull(T... objs){
        for (T obj : objs) {
            if (obj != null) {
                return false;
            }
        }
        return true;
    }
    public static <T> boolean allNonNull(T... objs){
        for (T obj : objs) {
            if (obj == null) {
                return false;
            }
        }
        return true;
    }
    public static <T> boolean containNullAndNonNull(T... objs){
        return !allNull(objs) && !allNonNull(objs);
    }

    public static String join(@NotNull String separator, Object... objs){
        StringBuilder sb=new StringBuilder();
        for (Object obj: objs){
            sb.append(obj).append(separator);
        }
        if (objs.length>0) {
            sb.deleteCharAt(sb.length() - separator.length());
        }
        return sb.toString();
    }
}