package org.smart4j.framework;

import org.smart4j.framework.helper.BeanHelper;
import org.smart4j.framework.helper.ClassHelper;
import org.smart4j.framework.helper.ControllerHelper;
import org.smart4j.framework.helper.IOCHelper;
import org.smart4j.framework.util.ClassUtil;

public final class HelperLoader {
	public static void init() {
		Class<?>[] classList = {
				ClassHelper.class,
				BeanHelper.class,
				IOCHelper.class,
				ControllerHelper.class
		};
		for(Class<?> cls : classList) {
			ClassUtil.loadClass(cls.getName(),false);
		}
		
	}
}