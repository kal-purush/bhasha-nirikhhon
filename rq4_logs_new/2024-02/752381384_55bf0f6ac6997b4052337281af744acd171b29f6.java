package com.example.utils.beans.factory;

import com.example.utils.beans.factory.stereotype.ComponentMarsel;
import com.example.utils.beans.factory.stereotype.RepositoryMarsel;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;

public class BeanFactoryMarsel {
    Map<String, Object> singletons = new HashMap<>();

    public Object getBeans(String beanName) {
        return singletons.get(beanName);
    }

    public void instantiate(String basePackage) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        String path = basePackage.replace('.', '/'); // "com.example" -> "com/example"
        try {
            Enumeration<URL> resources = classLoader.getResources(path);
            Iterator<URL> iterator = Collections.list(resources).iterator();
            while (iterator.hasNext()) {
                URL resource = iterator.next();
                File file = new File(resource.toURI());
                processDirectory(file, basePackage);
            }
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private void processDirectory(File directory, String packageName) {
        for (File file : directory.listFiles()) {
            String fileName = file.getName();
            if (file.isDirectory()) {
                processDirectory(file, packageName + "." + fileName); //Для поддиректорий
            } else if (fileName.endsWith(".class")) {
                String className = fileName.substring(0, fileName.lastIndexOf("."));
                try {
                    /**
                     * используем чтобы достать класс с помощью classLoader по имени и полному пути
                     */
                    Class<?> classObject = Class.forName(packageName + "." + className);
                    if (classObject.isAnnotationPresent(ComponentMarsel.class) || classObject.isAnnotationPresent(RepositoryMarsel.class)) {
                        System.out.println("Component: " + classObject);
                        Object instance = classObject.newInstance();
                        String beanName = className.substring(0, 1).toLowerCase() + className.substring(1);
                        singletons.put(beanName, instance);
                    }

                } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}