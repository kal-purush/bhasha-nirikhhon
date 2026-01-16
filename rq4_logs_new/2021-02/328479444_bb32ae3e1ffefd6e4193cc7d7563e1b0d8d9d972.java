package de.raidcraft.achievements.util;

import org.bukkit.Keyed;
import org.bukkit.NamespacedKey;

public final class EnumUtil {

    /**
     * Searches the given enum for a match based on the case insensitive name
     * or if it is an {@link Keyed} enum for its key.
     *
     * @param enumeration the enumeration class to search
     * @param search the value to search it with
     * @param <T> the type of the enum
     * @return the enum if a value was found. otherwise null.
     */
    public static <T extends Enum<?>> T searchEnum(Class<T> enumeration, String search) {

        for (T each : enumeration.getEnumConstants()) {
            if (each.name().compareToIgnoreCase(search) == 0) {
                return each;
            } else if (each instanceof Keyed) {
                NamespacedKey key = ((Keyed) each).getKey();
                if (key.getKey().equalsIgnoreCase(search)) {
                    return each;
                } else if (key.toString().equalsIgnoreCase(search)) {
                    return each;
                }
            }
        }

        return null;
    }

    private EnumUtil() {}
}