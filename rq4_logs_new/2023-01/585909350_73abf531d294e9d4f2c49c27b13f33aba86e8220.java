package com.serenebond.world;

import com.serenebond.main.Item;
import com.serenebond.tiles.BlendingTileType;
import com.serenebond.tiles.TileType;
import com.serenebond.tiles.WallTileType;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.HashMap;
import java.util.Map;

public final class Tiles {
    private static final Int2ObjectMap<TileType> COLOUR_TO_TILE;

    public static final TileType GRASS = new BlendingTileType(0, 48, 64, 0, 2);
    public static final TileType DIRT = new BlendingTileType(0, 0, 64, 48, 4);
    public static final TileType SAND = new BlendingTileType(16, 192, 112, 0, 3);
    public static final TileType STONE = new BlendingTileType(16, 176, 64, 96, 4);
    public static final TileType SNOW = new BlendingTileType(0, 16, 112, 48,1);
    public static final TileType COBBLESTONE = new BlendingTileType(37, 144, 112, 96, 4);

    // Foliage.
    public static final TileType PLANT = new BlendingTileType(0, 32, 64, 0, 2);
    public static final TileType RED_FLOWER = new BlendingTileType(0, 64, 64, 0, 2);
    public static final TileType ORANGE_FLOWER = new BlendingTileType(0, 80, 64, 0, 2);
    public static final TileType YELLOW_FLOWER = new BlendingTileType(0, 96, 64, 0, 2);
    public static final TileType LIGHT_BLUE_FLOWER = new BlendingTileType(0, 112, 64, 0, 2);
    public static final TileType DARK_BLUE_FLOWER = new BlendingTileType(0, 128, 64, 0, 2);
    public static final TileType PURPLE_FLOWER = new BlendingTileType(0, 144, 64, 0, 2);
    public static final TileType PINK_FLOWER = new BlendingTileType(0, 160, 64, 0, 2);
    public static final TileType WHITE_FLOWER = new BlendingTileType(0, 176, 64, 0, 2);

    // Can't walk on.
    public static final TileType MAGMA = new WallTileType(32, 176);
    public static final TileType LAVA = new WallTileType(48, 176);
    public static final TileType WATER = new WallTileType(48, 192);

    static {
        // Colours are Big Endian.
        var map = new Int2ObjectOpenHashMap<TileType>();
        map.put(0xFF84BA2C, GRASS);
        map.put(0xFF68422A, DIRT);
        map.put(0xFFF2CF79, SAND);
        map.put(0xFF908583, STONE);
        map.put(0xFFCAD4DB, SNOW);
        map.put(0xFF6A5A5A, COBBLESTONE);

        map.put(0xFF96C42C, PLANT);
        map.put(0xFFE0311A, RED_FLOWER);
        map.put(0xFFEC9914, ORANGE_FLOWER);
        map.put(0xFFECE814, YELLOW_FLOWER);
        map.put(0xFF14ECE5, LIGHT_BLUE_FLOWER);
        map.put(0xFF0575FF, DARK_BLUE_FLOWER);
        map.put(0xFFDB07F3, PURPLE_FLOWER);
        map.put(0xFFEF8FBC, PINK_FLOWER);
        map.put(0xFFF0E1F2, WHITE_FLOWER);

        map.put(0xFFFFBE1C, MAGMA);
        map.put(0xFFED2D18, LAVA);
        map.put(0xFF0094FF, WATER);

        COLOUR_TO_TILE = Int2ObjectMaps.unmodifiable(map);
    }

    private Tiles() {}

    public static TileType getTile(int colour) {
        return COLOUR_TO_TILE.getOrDefault(colour, GRASS);
    }

    // TODO:: Redo.
    public static Map<String, TileType> itemsTileType = new HashMap<>();
    static {
        itemsTileType.put(Item.stone.getID(), STONE);
        itemsTileType.put(Item.sand.getID(), SAND);
        itemsTileType.put(Item.dirt.getID(), DIRT);
        itemsTileType.put(Item.grass.getID(), GRASS);
        itemsTileType.put(Item.snow.getID(), SNOW);
        itemsTileType.put(Item.water.getID(), WATER);
    }
}