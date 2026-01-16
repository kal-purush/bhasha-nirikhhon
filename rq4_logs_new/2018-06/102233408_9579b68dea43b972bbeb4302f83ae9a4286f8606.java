package me.sirgregg.gchantbase.enchantsys;

import org.bukkit.ChatColor;
import org.bukkit.Material;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Enchant {
    String name();

    ChatColor color();

    Material[] applicable();

    int minLevel();

    int maxLevel();
}