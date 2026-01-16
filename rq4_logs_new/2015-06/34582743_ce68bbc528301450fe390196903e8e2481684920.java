package src.main.java;

import java.awt.ComponentOrientation;
import java.awt.Container;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;

import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

public class Components {

	public void addComponents(Container pane) {
		pane.setComponentOrientation(ComponentOrientation.LEFT_TO_RIGHT);

		pane.setLayout(new GridBagLayout());
		GridBagConstraints c = new GridBagConstraints();
		c.fill = GridBagConstraints.HORIZONTAL;

		JButton load = new JButton("Datei laden");
		c.fill = GridBagConstraints.HORIZONTAL;
		c.weightx = 0.5;
		c.gridx = 0;
		c.gridy = 0;
		c.gridwidth = GridBagConstraints.RELATIVE;
		pane.add(load, c);

		JButton save = new JButton("Datei speichern");
		c.fill = GridBagConstraints.HORIZONTAL;
		c.weightx = 0.5;
		c.gridx = 2;
		c.gridy = 0;
		c.gridwidth = 2;
		pane.add(save, c);

		JButton preview = new JButton("Vorschau");
		c.fill = GridBagConstraints.HORIZONTAL;
		c.weightx = 0.5;
		c.gridx = 2;
		c.gridy = 1;
		c.gridwidth = 2;
		pane.add(preview, c);

		JLabel collage = new JLabel("Collage:");
		c.fill = GridBagConstraints.VERTICAL;
		c.anchor = GridBagConstraints.CENTER;
		c.weightx = 0.5;
		c.gridx = 0;
		c.gridy = 1;
		c.gridwidth = 1;
		pane.add(collage, c);

		final JPanel collagePanel = new CollagePanel();
		final JLabel collageLabel = new JLabel();
		c.fill = GridBagConstraints.VERTICAL;
		c.anchor = GridBagConstraints.CENTER;
		c.weightx = 0.5;
		c.gridx = 0;
		c.gridy = 2;
		c.gridwidth = 3;
		c.gridheight = 3;
		collagePanel.add(collageLabel);
		pane.add(collagePanel, c);

		JTextField field = new JTextField(10);
		c.fill = GridBagConstraints.HORIZONTAL;
		c.weightx = 0.5;
		c.gridx = 1;
		c.gridy = 1;
		pane.add(field, c);

		load.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				final JFileChooser chooser = new JFileChooser();
				int value = chooser.showOpenDialog(null);
				if (value == JFileChooser.APPROVE_OPTION) {
					File file = chooser.getSelectedFile();

					ImageIcon icon = new ImageIcon(file.getPath());

					collageLabel.setIcon(icon);
				}
			}
		});

		// save.addActionListener(new ActionListener() {
		//
		// @Override
		// public void actionPerformed(ActionEvent e) {
		// final JFileChooser chooser = new JFileChooser();
		// int value = chooser.showSaveDialog(null);
		// if (value == JFileChooser.APPROVE_OPTION) {
		// File file = chooser.getSelectedFile();
		// ImageIO.write(collage, "jpg", new File(file.getPath()));
		// }
		// }
		// });

	}

}