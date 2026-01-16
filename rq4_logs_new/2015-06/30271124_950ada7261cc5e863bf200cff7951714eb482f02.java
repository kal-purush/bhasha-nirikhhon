package com.thaplayaslaya;

import java.awt.Color;
import java.awt.Component;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import javax.swing.JColorChooser;
import javax.swing.JDialog;
import javax.swing.colorchooser.AbstractColorChooserPanel;

/**
 * Take from user Boann from Stackoverflow
 * <http://stackoverflow.com/questions/12026767
 * /java-7-jcolorchooser-disable-transparency-slider>
 */

public class SwingUtil {
	/**
	 * Hides controls for configuring color transparency on the specified color
	 * chooser.
	 */
	public static void hideTransparencyControls(JColorChooser cc) {
		AbstractColorChooserPanel[] colorPanels = cc.getChooserPanels();
		for (int i = 0; i < colorPanels.length; i++) {
			AbstractColorChooserPanel cp = colorPanels[i];
			try {
				Field f = cp.getClass().getDeclaredField("panel");
				f.setAccessible(true);
				Object colorPanel = f.get(cp);

				Field f2 = colorPanel.getClass().getDeclaredField("spinners");
				f2.setAccessible(true);
				Object sliders = f2.get(colorPanel);

				Object transparencySlider = java.lang.reflect.Array.get(sliders, 3);
				if (i == colorPanels.length - 1)
					transparencySlider = java.lang.reflect.Array.get(sliders, 4);

				Method setVisible = transparencySlider.getClass().getDeclaredMethod("setVisible", boolean.class);
				setVisible.setAccessible(true);
				setVisible.invoke(transparencySlider, false);
			} catch (Throwable t) {
			}
		}
	}

	static Color[] result;
	static JColorChooser pane;

	/**
	 * Shows a modal color chooser dialog and blocks until the dialog is closed.
	 * 
	 * @param component
	 *            the parent component for the dialog; may be null
	 * @param title
	 *            the dialog's title
	 * @param initialColor
	 *            the initial color set when the dialog is shown
	 * @param showTransparencyControls
	 *            whether to show controls for configuring the color's
	 *            transparency
	 * @return the chosen color or null if the user canceled the dialog
	 */
	public static Color showColorChooserDialog(Component component, String title, Color initialColor, boolean showTransparencyControls) {
		pane = new JColorChooser(initialColor != null ? initialColor : Color.white);
		if (!showTransparencyControls)
			hideTransparencyControls(pane);
		result = new Color[1];
		ActionListener okListener = new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				result[0] = pane.getColor();
			}
		};
		@SuppressWarnings("static-access")
		JDialog dialog = pane.createDialog(component, title, true, pane, okListener, null);
		dialog.setVisible(true);
		dialog.dispose();
		return result[0];
	}
}