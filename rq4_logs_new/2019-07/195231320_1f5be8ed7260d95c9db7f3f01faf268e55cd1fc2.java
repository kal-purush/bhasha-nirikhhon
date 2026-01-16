package Model;

import org.junit.jupiter.api.Test;

import javax.swing.*;
import java.awt.*;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class ControlTypesTest {
    @Test
    void getObjectNotNullReturnTest() {
        ControlTypes control = ControlTypes.CHECKBOX;
        assertNotNull(control.getObject(false));
        assertNotNull(control.getObject(true));
        control = ControlTypes.TEXTFIELD;
        assertNotNull(control.getObject(false));
        assertNotNull(control.getObject(true));
        control = ControlTypes.RADIOBUTTON;
        assertNotNull(control.getObject(false));
        assertNotNull(control.getObject(true));
        control = ControlTypes.SPINNER;
        assertNotNull(control.getObject(false));
        assertNotNull(control.getObject(true));
        control = ControlTypes.SLIDER;
        assertNotNull(control.getObject(false));
        assertNotNull(control.getObject(true));
        control = ControlTypes.BUTTON;
        assertNotNull(control.getObject(false));
        assertNotNull(control.getObject(true));
    }
    @Test
    void getObjectCheckBoxWithSetValuesReturnTest() {
        ControlTypes control = ControlTypes.CHECKBOX;
        assertNotEquals("", ((JCheckBox) control.getObject(false)).getText());
        assertNotEquals("", ((JCheckBox) control.getObject(true)).getText());
    }
    @Test
    void getObjectTextFieldWithSetValuesReturnTest() {
        ControlTypes control = ControlTypes.TEXTFIELD;
        assertNotEquals("", ((JTextField) control.getObject(false)).getText());
        assertNotEquals("", ((JTextField) control.getObject(true)).getText());
    }
    @Test
    void getObjectRadioButtonWithSetValuesReturnTest() {
        ControlTypes control = ControlTypes.RADIOBUTTON;
        assertNotEquals("", ((JRadioButton) control.getObject(false)).getText());
        assertNotEquals("", ((JRadioButton) control.getObject(true)).getText());
    }
    @Test
    void getObjectSliderWithSetValuesReturnTest() {
        ControlTypes control = ControlTypes.SLIDER;
        assertEquals(JSlider.HORIZONTAL, ((JSlider) control.getObject(false)).getOrientation());
        assertNotEquals(new Dimension(0,0), control.getObject(false).getSize());
        assertEquals(JSlider.HORIZONTAL, ((JSlider) control.getObject(true)).getOrientation());
        assertNotEquals(new Dimension(0,0), control.getObject(true).getSize());
    }
    @Test
    void getObjectButtonWithSetValuesReturnTest() {
        ControlTypes control = ControlTypes.BUTTON;
        assertNotEquals("", ((JButton) control.getObject(false)).getText());
        assertNotEquals("", ((JButton) control.getObject(true)).getText());
    }
    @Test
    void getRandomObjectNotNullReturnTest() {
        ControlTypes control = ControlTypes.CHECKBOX;
        assertNotNull(control.getRandomObject());
        control = ControlTypes.TEXTFIELD;
        assertNotNull(control.getRandomObject());
        control = ControlTypes.RADIOBUTTON;
        assertNotNull(control.getRandomObject());
        control = ControlTypes.SPINNER;
        assertNotNull(control.getRandomObject());
        control = ControlTypes.SLIDER;
        assertNotNull(control.getRandomObject());
        control = ControlTypes.BUTTON;
        assertNotNull(control.getRandomObject());
    }
    @Test
    void getRandomObjectControlFromEnumReturnTest() {
        ControlTypes control = ControlTypes.BUTTON;
        ArrayList list = new ArrayList<>(Arrays.asList(ControlTypes.values()));
        assertTrue(list.contains(control.getRandomObject()));
    }
    @Test
    void getFolderNameReturnTest() {
        ControlTypes control = ControlTypes.CHECKBOX;
        assertEquals("checkbox", control.getFolderName());
        control = ControlTypes.TEXTFIELD;
        assertEquals("textfield", control.getFolderName());
        control = ControlTypes.RADIOBUTTON;
        assertEquals("radiobutton", control.getFolderName());
        control = ControlTypes.SPINNER;
        assertEquals("spinner", control.getFolderName());
        control = ControlTypes.SLIDER;
        assertEquals("slider", control.getFolderName());
        control = ControlTypes.BUTTON;
        assertEquals("button", control.getFolderName());
    }
}