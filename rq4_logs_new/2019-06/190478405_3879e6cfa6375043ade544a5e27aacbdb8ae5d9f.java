package com.github.code31415926535.game;

import java.awt.*;
import java.io.File;
import java.io.IOException;

final class Fonts {
    private static final String FONTS_DIR_ROOT = "fonts";
    private static final Font LYCANTHROPE = loadFont("Lycanthrope.ttf");

    static final Font MENU_FONT = LYCANTHROPE.deriveFont(32F);
    static final Font TITLE_FONT = LYCANTHROPE.deriveFont(64F);

    private static Font loadFont(String name) {
        return new Fonts()._loadFont(name);
    }

    private Fonts() {}

    private Font _loadFont(String name) {
        String filename = getClass().getClassLoader().getResource(FONTS_DIR_ROOT + "/" + name).getFile();
        System.out.println(filename);
        File file = new File(filename);
        try {
            return Font.createFont(Font.TRUETYPE_FONT, file);
        } catch (FontFormatException | IOException e) {
            e.printStackTrace();
        }

        return null;
    }
}