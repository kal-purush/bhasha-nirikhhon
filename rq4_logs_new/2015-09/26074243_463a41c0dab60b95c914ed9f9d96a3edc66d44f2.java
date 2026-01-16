package org.codelogger.core.context.bean;

import static org.codelogger.utils.StringUtils.isNotBlank;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.LinkedHashSet;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.codelogger.core.context.stereotype.Autowired;
import org.codelogger.utils.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;

public class ComponentScanner {

  public ComponentScanner(final ConcurrentHashMap<Class<?>, Object> typeToBean,
    final Class<? extends Annotation>... componentTypes) {

    this.typeToBean = typeToBean == null ? new ConcurrentHashMap<Class<?>, Object>() : typeToBean;
    this.componentTypes = componentTypes;
    classLoader = getClass().getClassLoader();
  }

  public ComponentScanner(final ClassLoader classLoader,
    final ConcurrentHashMap<Class<?>, Object> typeToBean,
    final Class<? extends Annotation>... componentTypes) {

    this.classLoader = classLoader;
    this.typeToBean = typeToBean == null ? new ConcurrentHashMap<Class<?>, Object>() : typeToBean;
    this.componentTypes = componentTypes;
  }

  public ConcurrentHashMap<Class<?>, Object> scan(final Properties configurations,
    final ConcurrentHashMap<Class<?>, Object> typeToBean) {

    this.typeToBean.putAll(typeToBean);
    return scan(configurations);
  }

  public ConcurrentHashMap<Class<?>, Object> scan(final Properties configurations) {

    if (MapUtils.isEmpty(configurations)) {
      throw new IllegalArgumentException("Argument configLocation can not be empty.");
    }
    try {
      ClassPath classPath = ClassPath.from(classLoader);
      String valueOfScanPackage = configurations.getProperty(SCAN_PACKAGE);
      if (isNotBlank(valueOfScanPackage)) {
        logger.debug("scan components for packages[{}]", valueOfScanPackage);
        String[] basePackages = valueOfScanPackage.split(INIT_PARAM_DELIMITERS);
        Set<ClassInfo> allClassInfo = new LinkedHashSet<ClassInfo>();
        for (String basePackage : basePackages) {
          allClassInfo.addAll(classPath.getTopLevelClassesRecursive(basePackage));
        }
        for (ClassInfo classInfo : allClassInfo) {
          Class<?> load = classInfo.load();
          for (Class<? extends Annotation> componentType : componentTypes) {
            if (load.isAnnotationPresent(componentType)) {
              typeToBean.put(load, load.newInstance());
            }
          }
        }
        for (Entry<Class<?>, Object> classWithInstance : typeToBean.entrySet()) {
          Class<?> targetClass = classWithInstance.getKey();
          Object targetInstance = classWithInstance.getValue();
          for (Field field : targetClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(Autowired.class)) {
              System.out.println("==========set value==========");
              logger.debug("field {}", field.getGenericType());
              if (Modifier.isFinal(field.getModifiers())) {
                continue;
              }
              field.setAccessible(true);
              logger.debug("target {}", targetInstance);
              Object value = typeToBean.get(field.getGenericType());
              logger.debug("value {}", value);
              field.set(targetInstance, value);
            }
          }
        }
      }
      return typeToBean;
    } catch (Exception e) {
      logger.error("Init application context failed.", e);
      throw new IllegalArgumentException();
    }
  }

  public Class<? extends Annotation>[] getComponentTypes() {

    return componentTypes;
  }

  private final ClassLoader classLoader;

  private final ConcurrentHashMap<Class<?>, Object> typeToBean;

  private final Class<? extends Annotation>[] componentTypes;

  private static final String INIT_PARAM_DELIMITERS = "[,; \t\n]";

  private static final String SCAN_PACKAGE = "scan-package";

  private static final Logger logger = LoggerFactory.getLogger(ComponentScanner.class);
}