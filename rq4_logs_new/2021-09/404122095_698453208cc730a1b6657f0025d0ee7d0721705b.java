package com.item;

import org.json.simple.parser.ParseException;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;

import static org.junit.Assert.*;

public class WeaponTest {

    @Test
    public void whenOfferRightFileToGetWeapons_shouldReturnHashMapWithEntries() throws IOException {
        String filePath = "cfg/Weapons.json";
        HashMap<String, Weapon> weaponsMap = Weapon.getWeapons(filePath);
        assertEquals(2, weaponsMap.size());
    }

    @Test(expected = IOException.class)
    public void testGetWeaponsFileNotFoundException() throws IOException {
        String filePath = "abc/Weapons.json";
        HashMap<String, Weapon> weaponsMap = Weapon.getWeapons(filePath);
    }
}