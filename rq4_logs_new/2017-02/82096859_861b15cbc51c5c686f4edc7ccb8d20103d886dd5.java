package com.makarov.core;

import com.makarov.mapper.manager.api.TypeManager;
import com.makarov.mapper.manager.impl.DefaultTypeManager;

import java.util.HashMap;
import java.util.Map;

/**
 * Class for getting type manager
 *
 * @author Makarov Alexey
 * @version 1.0
 */
public class TypeConfigurator {

    private static Map<String, TypeManager> typeManagerMap = new HashMap<String, TypeManager>() {
        {
            put("default", new DefaultTypeManager());
        }
    };

    private static String currentDataBase = "default";

    /**
     * Set type manager for concrete database driver
     *
     * @param currentDataBase - current used database driver
     */
    static void setCurrentDataBase(String currentDataBase) {
        if (typeManagerMap.get(currentDataBase) != null) {
            TypeConfigurator.currentDataBase = currentDataBase;
        }
    }

    /**
     * Get current type manager
     *
     * @return current type manager
     */
    public static TypeManager getTypeManager() {
        return typeManagerMap.get(currentDataBase);
    }
}