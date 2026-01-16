package me.yummykang;

import me.yummykang.annotation.Controller;
import me.yummykang.helper.BeanHelper;
import me.yummykang.helper.ClassHelper;
import me.yummykang.helper.ControllerHelper;
import me.yummykang.helper.IocHelper;
import me.yummykang.utils.ClassUtil;

/**
 * write some dec. here.
 * Created by Demon on 2016/11/19 0019.
 */
public class ContextInit {
    public static void init() {
        Class<?>[] classes = {ClassHelper.class, BeanHelper.class, IocHelper.class, ControllerHelper.class};
        for (Class<?> clazz : classes) {
            ClassUtil.loadClass(clazz.getName(), false);
        }
    }
}