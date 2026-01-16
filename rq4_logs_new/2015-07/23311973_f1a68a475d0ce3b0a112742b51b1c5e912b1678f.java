package org.gnode.nix;

import org.gnode.nix.util.Util;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class TestUtil {

    @Before
    public void setUp() {

    }

    @After
    public void tearDown() {

    }

    @Test
    public void testStrUtils() {
        String a = " a ";
        String b = "\tb\t";
        String c = " c c\tc ";

        a = Util.deblankString(a);
        b = Util.deblankString(b);
        c = Util.deblankString(c);

        assertEquals("a", a);
        assertEquals("b", b);
        assertEquals("ccc", c);
    }

    @Test
    public void testUnitScaling() {
        try {
            Util.getSIScaling("mOhm", "ms");
            fail();
        } catch (RuntimeException re) {
        }

        assertTrue(Util.getSIScaling("mV", "kV") == 1e-6);
        assertTrue(Util.getSIScaling("V", "kV") == 1e-03);
        assertTrue(Util.getSIScaling("kV", "V") == 1e+03);

        try {
            Util.getSIScaling("mV^2", "V");
            fail();
        } catch (RuntimeException re) {
        }

        assertTrue(Util.getSIScaling("V^2", "V^2") == 1.0);
        assertTrue(Util.getSIScaling("V", "mV") == 1e+03);
        assertTrue(Util.getSIScaling("V^2", "mV^2") == 1e+06);
        assertTrue(Util.getSIScaling("mV^2", "kV^2") == 1e-12);
    }

    @Test
    public void testIsSIUnit() {
        assertTrue(Util.isSIUnit("V"));
        assertTrue(Util.isSIUnit("mV"));
        assertTrue(Util.isSIUnit("mV^-2"));
        assertTrue(Util.isSIUnit("mV/cm"));
        assertTrue(Util.isSIUnit("dB"));
        assertTrue(Util.isSIUnit("rad"));
    }

    @Test
    public void testSIUnitSplit() {
        String unit_1 = "V";
        String unit_2 = "mV";
        String unit_3 = "mV^2";
        String unit_4 = "mV^-2";
        String unit_5 = "m^2";

        String[] res = Util.splitUnit(unit_1);
        assertTrue(res[0].equals("") && res[1].equals("V") && res[2].equals(""));

        res = Util.splitUnit(unit_2);
        assertTrue(res[0].equals("m") && res[1].equals("V") && res[2].equals(""));

        res = Util.splitUnit(unit_3);
        assertTrue(res[0].equals("m") && res[1].equals("V") && res[2].equals("2"));

        res = Util.splitUnit(unit_4);
        assertTrue(res[0].equals("m") && res[1].equals("V") && res[2].equals("-2"));

        res = Util.splitUnit(unit_5);
        assertTrue(res[0].equals("") && res[1].equals("m") && res[2].equals("2"));
    }

    @Test
    public void testIsAtomicSIUnit() {
        assertTrue(Util.isAtomicSIUnit("V"));
        assertTrue(Util.isAtomicSIUnit("mV"));
        assertTrue(Util.isAtomicSIUnit("mV^-2"));
        assertFalse(Util.isAtomicSIUnit("mV/cm"));
        assertTrue(Util.isAtomicSIUnit("dB"));
        assertTrue(Util.isAtomicSIUnit("rad"));
    }

    @Test
    public void testIsCompoundSIUnit() {
        String unit_1 = "mV*cm^-2";
        String unit_2 = "mV/cm^2";
        String unit_3 = "mV/cm^2*kg";
        String unit_4 = "mV";

        assertTrue(Util.isCompoundSIUnit(unit_1));
        assertTrue(Util.isCompoundSIUnit(unit_2));
        assertTrue(Util.isCompoundSIUnit(unit_3));
        assertFalse(Util.isCompoundSIUnit(unit_4));
    }

    @Test
    public void testSplitCompoundUnit() {
        String unit = "mV/cm^2*kg*V";
        String unit_2 = "mOhm/m";
        String unit_3 = "mV";

        List<String> atomic_units = Util.splitCompoundUnit(unit);
        assertTrue(atomic_units.size() == 4);
        assertEquals(atomic_units.get(0), "mV");
        assertEquals(atomic_units.get(1), "cm^-2");
        assertEquals(atomic_units.get(2), "kg");
        assertEquals(atomic_units.get(3), "V");

        List<String> atomic_units_2 = Util.splitCompoundUnit(unit_2);
        assertTrue(atomic_units_2.size() == 2);
        assertEquals(atomic_units_2.get(0), "mOhm");
        assertEquals(atomic_units_2.get(1), "m^-1");

        List<String> atomic_units_3 = Util.splitCompoundUnit(unit_3);
        assertTrue(atomic_units_3.size() == 1);
        assertEquals(atomic_units_3.get(0), unit_3);
    }

    @Test
    public void testConvertToSeconds() {
        String unit_min = "min";
        String unit_h = "h";
        String unit_s = "s";
        String unit_ms = "ms";
        String unit_Ms = "Ms";
        double min_value = 25.5;
        double h_value = 12.25;
        double s_value = 100;
        long m_value = 25;
        assertTrue(1530.0 == Util.convertToSeconds(unit_min, min_value));
        assertTrue(44100.0 == Util.convertToSeconds(unit_h, h_value));
        assertTrue(1500 == Util.convertToSeconds(unit_min, m_value));
        assertTrue(s_value == Util.convertToSeconds(unit_s, s_value));
        assertTrue(s_value / 1000 == Util.convertToSeconds(unit_ms, s_value));
        assertTrue(s_value * 1000000 == Util.convertToSeconds(unit_Ms, s_value));
        assertTrue(s_value == Util.convertToSeconds("std", s_value));
    }

    @Test
    public void testConvertToKelvin() {
        String unit_f = "°F";
        String unit_f2 = "F";
        String unit_c = "°C";
        String unit_c2 = "C";
        String unit_k = "K";
        String unit_mk = "mK";
        String unit_Mk = "MK";
        String unit_k2 = "°K";
        double temperature = 100.0;
        assertTrue(373.15 == Util.convertToKelvin(unit_c, temperature));
        assertTrue(373.15 == Util.convertToKelvin(unit_c2, temperature));
        assertTrue(311.0 == Math.round(Util.convertToKelvin(unit_f, temperature)));
        assertTrue(311.0 == Math.round(Util.convertToKelvin(unit_f2, temperature)));
        assertTrue(temperature == Util.convertToKelvin(unit_k, temperature));
        assertTrue(temperature == Util.convertToKelvin(unit_k2, temperature));
        assertTrue(temperature / 1000 == Util.convertToKelvin(unit_mk, temperature));
        assertTrue(temperature * 1000000 == Util.convertToKelvin(unit_Mk, temperature));
        assertTrue(temperature == Util.convertToKelvin("kelvin", temperature));
        int temp_fi = 100;
        assertTrue(Util.convertToKelvin(unit_f, temp_fi) == 311);
    }

    @Test
    public void testUnitSanitizer() {
        String unit = " mul/µs ";
        assertEquals(Util.unitSanitizer(unit), "ul/us");
    }
}